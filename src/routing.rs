use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct RoutingState {
    claims: HashMap<String, ClaimRoute>,
}

impl RoutingState {
    pub fn claim_recipient(&self, device_id: &str) -> Option<ClaimRecipient<'_>> {
        let claim = self.claims.get(device_id)?;
        Some(ClaimRecipient {
            endpoint: &claim.controller_endpoint,
            session_id: &claim.controller_session_id,
        })
    }

    pub fn reconcile_snapshot(
        &mut self,
        next_claims: HashMap<String, ClaimRoute>,
        invalid_claim_devices: HashSet<String>,
    ) -> HashSet<String> {
        let devices_to_reset =
            self.devices_to_reset_for_snapshot(&next_claims, &invalid_claim_devices);
        self.claims = next_claims;
        devices_to_reset
    }

    pub fn remove_device(&mut self, device_id: &str) {
        self.claims.remove(device_id);
    }

    fn devices_to_reset_for_snapshot(
        &self,
        next_claims: &HashMap<String, ClaimRoute>,
        invalid_claim_devices: &HashSet<String>,
    ) -> HashSet<String> {
        let mut devices_to_reset = invalid_claim_devices.clone();
        for (device_id, old_claim) in &self.claims {
            let Some(next_claim) = next_claims.get(device_id) else {
                devices_to_reset.insert(device_id.clone());
                continue;
            };
            if claim_route_identity(old_claim) != claim_route_identity(next_claim) {
                devices_to_reset.insert(device_id.clone());
                continue;
            }
        }
        devices_to_reset
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRoute {
    pub controller_endpoint: String,
    pub controller_session_id: String,
    pub contract_key: String,
    pub claim_id: String,
}

fn claim_route_identity(claim: &ClaimRoute) -> (&str, &str, &str, &str) {
    (
        &claim.controller_endpoint,
        &claim.controller_session_id,
        &claim.contract_key,
        &claim.claim_id,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimRecipient<'a> {
    pub endpoint: &'a str,
    pub session_id: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(endpoint: &str, session: &str) -> ClaimRoute {
        ClaimRoute {
            controller_endpoint: endpoint.to_string(),
            controller_session_id: session.to_string(),
            contract_key: "contracts.claim.1.meta".to_string(),
            claim_id: "claim-1".to_string(),
        }
    }

    #[test]
    fn claim_recipient_uses_valid_concord_route() {
        let mut routing = RoutingState::default();
        routing.reconcile_snapshot(
            HashMap::from([("fip".to_string(), claim("controller:main", "s1"))]),
            HashSet::new(),
        );
        assert_eq!(
            routing.claim_recipient("fip"),
            Some(ClaimRecipient {
                endpoint: "controller:main",
                session_id: "s1"
            })
        );
    }

    #[test]
    fn delete_and_session_mismatch_reset_devices() {
        let mut routing = RoutingState::default();
        routing.reconcile_snapshot(
            HashMap::from([("fip".to_string(), claim("controller:main", "s1"))]),
            HashSet::new(),
        );

        let reset = routing.reconcile_snapshot(HashMap::new(), HashSet::new());
        assert!(reset.contains("fip"));

        routing.reconcile_snapshot(
            HashMap::from([("fip".to_string(), claim("controller:main", "s1"))]),
            HashSet::new(),
        );
        let reset = routing.reconcile_snapshot(
            HashMap::from([("fip".to_string(), claim("controller:main", "s1"))]),
            HashSet::new(),
        );
        assert!(!reset.contains("fip"));
    }

    #[test]
    fn invalid_claim_resets_and_is_not_recorded() {
        let mut routing = RoutingState::default();
        let reset = routing.reconcile_snapshot(HashMap::new(), HashSet::from(["fip".to_string()]));
        assert!(reset.contains("fip"));
        assert_eq!(routing.claim_recipient("fip"), None);
    }
}
