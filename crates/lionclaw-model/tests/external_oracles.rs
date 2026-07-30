use std::collections::BTreeMap;

use lionclaw_model::{
    AuthorityCeilings, ConfinementResources, ExecutionPolicy, ExternalOracle,
    ExternalOracleDriverId, OracleSpec, OracleSpecError,
};

fn external(fields: &[(&str, &str)]) -> OracleSpec {
    OracleSpec::External(ExternalOracle {
        driver: ExternalOracleDriverId::new("local-ci").unwrap(),
        request: fields
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        timeout_secs: 120,
        poll_secs: 5,
    })
}

#[test]
fn external_oracle_digest_is_bound_to_driver_and_request() {
    let base = external(&[("suite", "cargo-test"), ("artifact", "workspace")]);
    let changed_request = external(&[("suite", "cargo-clippy"), ("artifact", "workspace")]);
    let changed_driver = OracleSpec::External(ExternalOracle {
        driver: ExternalOracleDriverId::new("local-security").unwrap(),
        request: BTreeMap::from([("suite".to_string(), "cargo-test".to_string())]),
        timeout_secs: 120,
        poll_secs: 5,
    });

    assert_ne!(base.digest(), changed_request.digest());
    assert_ne!(base.digest(), changed_driver.digest());
}

#[test]
fn external_oracle_rejects_paths_shells_and_credentials() {
    for raw in ["./driver", "/bin/check", "local ci", "driver;sh"] {
        assert!(matches!(
            ExternalOracleDriverId::new(raw),
            Err(OracleSpecError::InvalidExternalDriverId(_))
        ));
    }

    let spec = external(&[("api_token", "must-not-enter-the-mission")]);
    assert!(matches!(
        spec.validate(
            &AuthorityCeilings::default(),
            &ConfinementResources::default(),
            &ExecutionPolicy::default()
        ),
        Err(OracleSpecError::ExternalRequestContainsCredential(_))
    ));
}

#[test]
fn external_oracle_polling_is_bounded_by_timeout() {
    let spec = OracleSpec::External(ExternalOracle {
        driver: ExternalOracleDriverId::new("local-ci").unwrap(),
        request: BTreeMap::from([("suite".to_string(), "cargo-test".to_string())]),
        timeout_secs: 30,
        poll_secs: 31,
    });

    assert!(matches!(
        spec.validate(
            &AuthorityCeilings::default(),
            &ConfinementResources::default(),
            &ExecutionPolicy::default()
        ),
        Err(OracleSpecError::InvalidExternalPoll { .. })
    ));
}
