use super::*;
use crate::toolchain::envdiff::{EnvReport, Observed};
use crate::toolchain::versions::{Constraint, Match, ToolId, VersionObservation};

fn status(report: &EnvReport, required: ToolRequirement) -> Result<ToolStatus, ApiError> {
    let tool =
        ToolId::parse(&required.name).map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?;
    let constraint = Constraint::parse(&required.version)
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?;
    let fact = report
        .tools
        .get(&required.name)
        .or_else(|| {
            report.tools.iter().find_map(|(name, fact)| {
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
        (ToolMatch::Missing, None)
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
            .map(|r| status(&report, r))
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
