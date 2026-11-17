use super::*;
use crate::toolchain::envdiff::{Observed, ToolFact};
use crate::toolchain::versions::{Constraint, Match, ToolId, VersionObservation};

fn status(
    tools: &BTreeMap<String, ToolFact>,
    required: ToolRequirement,
) -> Result<ToolStatus, ApiError> {
    let tool =
        ToolId::parse(&required.name).map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?;
    let constraint = Constraint::parse(&required.version)
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?;
    let fact = tools
        .get(&required.name)
        .or_else(|| {
            tools.iter().find_map(|(name, fact)| {
                (crate::toolchain::contract::canonical_tool(name) == tool.as_str()).then_some(fact)
            })
        })
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::InvalidInput,
                "tool is outside the measured environment",
            )
        })?;
    let matches =
        |version: &str| constraint.matches(&VersionObservation::parse_for(&tool, version));
    let managed = fact
        .managed
        .iter()
        .flat_map(|m| &m.installed)
        .find(|v| matches(v) == Match::Match);
    let (status, installed) = if let Some(version) = managed {
        (ToolMatch::Match, Some(version.clone()))
    } else if fact.observed == Observed::Unavailable {
        match fact.managed.as_ref().and_then(|m| m.installed.first()) {
            Some(version)
                if fact
                    .managed
                    .iter()
                    .flat_map(|m| &m.installed)
                    .all(|v| matches(v) == Match::Mismatch) =>
            {
                (ToolMatch::VersionMismatch, Some(version.clone()))
            }
            Some(_) => {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    "worker could not determine managed tool version compatibility",
                ))
            }
            None => (ToolMatch::Missing, None),
        }
    } else {
        let decision = fact
            .version
            .as_deref()
            .filter(|_| fact.observed == Observed::Available)
            .map(matches);
        let status = match decision {
            Some(Match::Match) => ToolMatch::Match,
            Some(Match::Mismatch) => ToolMatch::VersionMismatch,
            _ => {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    "worker could not determine tool version compatibility",
                ))
            }
        };
        (status, fact.version.clone())
    };
    Ok(ToolStatus {
        name: required.name,
        required: required.version,
        installed,
        adapter: None,
        status,
    })
}

impl Agent {
    pub(super) fn env_report(
        &self,
        request: EnvReportRequest,
    ) -> Result<EnvReportResponse, ApiError> {
        if request.required.len() > 128 {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "too many environment requirements",
            ));
        }
        let profile = self.cfg.capability_profile.as_ref().ok_or_else(|| {
            ApiError::new(
                ErrorCode::Conflict,
                "worker has no active execution profile",
            )
        })?;
        let report = crate::toolchain::envreport::collect_profile(
            &self.cfg.state_dir,
            profile,
            &profile.host_environment(),
        );
        let tools = request
            .required
            .into_iter()
            .map(|r| status(&report.tools, r))
            .collect::<Result<_, _>>()?;
        Ok(EnvReportResponse {
            schema_version: SchemaVersion::CURRENT,
            profile_fingerprint: self.cfg.profile_fingerprint.clone(),
            os: report.os.clone(),
            arch: report.arch.clone(),
            tools,
            environment: Some(report),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolchain::envdiff::Managed;

    #[test]
    fn requirement_status_keeps_unmeasured_missing_and_other_installed_versions_distinct() {
        let check = |observed, version: Option<&str>, installed: &[&str]| {
            let fact = ToolFact {
                observed,
                version: version.map(str::to_owned),
                managed: Some(Managed {
                    installed: installed.iter().map(|s| s.to_string()).collect(),
                    inventory: vec!["22.1.0".into()],
                }),
            };
            let facts = BTreeMap::from([("node".into(), fact)]);
            status(
                &facts,
                ToolRequirement {
                    name: "nodejs".into(),
                    version: "22".into(),
                },
            )
        };
        let satisfied = check(Observed::Unknown, None, &["20.0.0", "22.1.0"]).unwrap();
        assert_eq!(satisfied.status, ToolMatch::Match);
        assert_eq!(satisfied.installed.as_deref(), Some("22.1.0"));
        assert_eq!(
            check(Observed::Unknown, None, &[]).unwrap_err().code,
            ErrorCode::Conflict
        );
        assert_eq!(
            check(Observed::Unavailable, None, &[]).unwrap().status,
            ToolMatch::Missing
        );
        let other = check(Observed::Unavailable, None, &["20.0.0"]).unwrap();
        assert_eq!(other.status, ToolMatch::VersionMismatch);
        assert_eq!(other.installed.as_deref(), Some("20.0.0"));
        assert_eq!(
            check(Observed::Available, Some("22.0.1"), &[])
                .unwrap()
                .status,
            ToolMatch::Match
        );
    }
}
