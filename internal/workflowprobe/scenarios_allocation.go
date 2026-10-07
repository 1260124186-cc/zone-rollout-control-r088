package workflowprobe

import (
	"encoding/json"
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeLeaseAllocation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-east-02", "east"); err != nil {
		return err
	}
	readyPage, err := listNodes(harness, "?state=ready&region=east")
	if err != nil {
		return err
	}
	if len(readyPage.Items) != 2 {
		return ensure(false, "ready node list must contain both nodes")
	}
	for _, item := range readyPage.Items {
		if item.ActiveLease != nil {
			return ensure(false, "ready node must not show an active lease")
		}
	}
	if err := ensureActiveLeaseFieldNull(harness, "?state=ready&region=east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "lease.example", 1001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	first, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(first.LeaseIDs) != 2 {
		return ensure(false, "first lease group must contain two leases")
	}
	if err := ensureLeasedNodesExposeLease(harness, plan.ID, first.LeaseIDs, harness.clock.Now().Add(600*time.Second)); err != nil {
		return err
	}
	replay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "lease request replay returned another group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0002", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	afterRelease, err := listNodes(harness, "?region=east")
	if err != nil {
		return err
	}
	stillLeased := 0
	for _, item := range afterRelease.Items {
		switch item.State {
		case domain.NodeReady:
			if item.ActiveLease != nil {
				return ensure(false, "released node must not show an active lease")
			}
		case domain.NodeLeased:
			if item.ActiveLease == nil {
				return ensure(false, "leased node must keep its active lease")
			}
			if item.ActiveLease.LeaseID != first.LeaseIDs[1] {
				return ensure(false, "remaining active lease must be the unreleased one")
			}
			stillLeased++
		}
	}
	if stillLeased != 1 {
		return ensure(false, "exactly one node must remain leased after release")
	}
	return nil
}

func ensureLeasedNodesExposeLease(
	harness *harness,
	planID string,
	leaseIDs []string,
	expectedExpiry time.Time,
) error {
	seen := make(map[string]bool)
	check := func(item domain.NodeView) error {
		if item.ActiveLease == nil {
			return ensure(false, "leased node must show its active lease")
		}
		if item.ActiveLease.PlanID != planID {
			return ensure(false, "active lease must reference the allocating plan")
		}
		if !item.ActiveLease.ExpiresAt.Equal(expectedExpiry.UTC()) {
			return ensure(false, "active lease must expose the lease deadline")
		}
		seen[item.ActiveLease.LeaseID] = true
		return nil
	}
	firstPage, err := listNodes(harness, "?state=leased&region=east&limit=1")
	if err != nil {
		return err
	}
	if len(firstPage.Items) != 1 || firstPage.Meta.NextCursor == "" {
		return ensure(false, "leased node list must paginate with a cursor")
	}
	if err := check(firstPage.Items[0]); err != nil {
		return err
	}
	secondPage, err := listNodes(harness, "?state=leased&region=east&limit=1&cursor="+firstPage.Meta.NextCursor)
	if err != nil {
		return err
	}
	if len(secondPage.Items) != 1 || secondPage.Meta.NextCursor != "" {
		return ensure(false, "second leased node page must contain the remaining node")
	}
	if secondPage.Items[0].ID == firstPage.Items[0].ID {
		return ensure(false, "pagination must not repeat nodes")
	}
	if err := check(secondPage.Items[0]); err != nil {
		return err
	}
	for _, leaseID := range leaseIDs {
		if !seen[leaseID] {
			return ensure(false, "node list must surface the allocated lease identifiers")
		}
	}
	return nil
}

func ensureActiveLeaseFieldNull(harness *harness, query string) error {
	var page struct {
		Items []struct {
			ActiveLease json.RawMessage `json:"active_lease"`
		} `json:"items"`
	}
	if err := harness.call(http.MethodGet, "/v1/nodes"+query, nil, &page); err != nil {
		return err
	}
	for _, item := range page.Items {
		if string(item.ActiveLease) != "null" {
			return ensure(false, "node without a lease must expose active_lease as null")
		}
	}
	return nil
}
