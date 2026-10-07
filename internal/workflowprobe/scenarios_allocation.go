package workflowprobe

import (
	"net/http"

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
	readyBefore, err := listNodes(harness, "?state=ready&region=east")
	if err != nil {
		return err
	}
	if len(readyBefore.Items) != 2 {
		return ensure(false, "ready node listing must contain both nodes")
	}
	for _, item := range readyBefore.Items {
		if item.ActiveLease != nil {
			return ensure(false, "ready node must not carry an active lease")
		}
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
	leased, err := listNodes(harness, "?state=leased&region=east")
	if err != nil {
		return err
	}
	if len(leased.Items) != 2 {
		return ensure(false, "leased node listing must contain both nodes")
	}
	for _, item := range leased.Items {
		if item.State != domain.NodeLeased {
			return ensure(false, "leased listing returned a node in another state")
		}
		if item.ActiveLease == nil {
			return ensure(false, "leased node must carry its active lease")
		}
		if item.ActiveLease.PlanID != plan.ID {
			return ensure(false, "active lease must reference the leasing plan")
		}
		if item.ActiveLease.LeaseID != first.LeaseIDs[0] && item.ActiveLease.LeaseID != first.LeaseIDs[1] {
			return ensure(false, "active lease must reference a lease from the allocation group")
		}
		if item.ActiveLease.ExpiresAt.IsZero() {
			return ensure(false, "active lease must expose its expiry time")
		}
	}
	paged, err := listNodes(harness, "?state=leased&region=east&limit=1")
	if err != nil {
		return err
	}
	if len(paged.Items) != 1 || paged.Meta.NextCursor == "" {
		return ensure(false, "paged leased listing must return one node and a cursor")
	}
	if paged.Items[0].ActiveLease == nil {
		return ensure(false, "paged leased node must carry its active lease")
	}
	rest, err := listNodes(harness, "?state=leased&region=east&limit=1&cursor="+paged.Meta.NextCursor)
	if err != nil {
		return err
	}
	if len(rest.Items) != 1 || rest.Items[0].ID == paged.Items[0].ID {
		return ensure(false, "cursor page must return the remaining leased node")
	}
	emptyRegion, err := listNodes(harness, "?state=leased&region=west")
	if err != nil {
		return err
	}
	if len(emptyRegion.Items) != 0 {
		return ensure(false, "region filter must still narrow the node set")
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
	readyAfter, err := listNodes(harness, "?state=ready&region=east")
	if err != nil {
		return err
	}
	if len(readyAfter.Items) != 1 {
		return ensure(false, "released node must be listed as ready")
	}
	if readyAfter.Items[0].ActiveLease != nil {
		return ensure(false, "released node must not keep an active lease")
	}
	stillLeased, err := listNodes(harness, "?state=leased&region=east")
	if err != nil {
		return err
	}
	if len(stillLeased.Items) != 1 {
		return ensure(false, "one node must remain leased")
	}
	if stillLeased.Items[0].ActiveLease == nil || stillLeased.Items[0].ActiveLease.LeaseID != first.LeaseIDs[1] {
		return ensure(false, "remaining leased node must keep its active lease")
	}
	return nil
}
