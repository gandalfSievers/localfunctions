/// Result of virtual hosted-style host parsing.
#[derive(Debug, PartialEq)]
pub struct VirtualHostInfo<'a> {
    pub function_name: &'a str,
    pub region: Option<&'a str>,
}

/// Extract a function name (and optionally region) from the Host header for
/// virtual hosted-style requests.
///
/// Supports two formats (tried in order):
/// 1. AWS-style: `<function>.lambda.<region>.amazonaws.com`
/// 2. Custom domain: `<function>.<domain>` (e.g., `my-func.lambda.local`)
///
/// AWS-style is tried first since it is more specific — a custom domain like
/// `amazonaws.com` would otherwise swallow the region component.
///
/// Returns `None` if neither format matches (fall back to path-based routing).
pub fn extract_function_from_host<'a>(
    host: &'a str,
    domain: Option<&str>,
) -> Option<VirtualHostInfo<'a>> {
    // Strip port from host if present (e.g., "my-func.lambda.us-east-1.amazonaws.com:9600")
    let host_without_port = host.split(':').next().unwrap_or(host);

    // Try AWS-style first: <function>.lambda.<region>.amazonaws.com
    if let Some(fn_and_rest) = host_without_port.strip_suffix(".amazonaws.com") {
        // fn_and_rest is e.g. "my-func.lambda.us-east-1"
        if let Some(dot_lambda_pos) = fn_and_rest.find(".lambda.") {
            let function_name = &fn_and_rest[..dot_lambda_pos];
            let region = &fn_and_rest[dot_lambda_pos + 8..]; // skip ".lambda."
            if !function_name.is_empty() && !region.is_empty() {
                return Some(VirtualHostInfo {
                    function_name,
                    region: Some(region),
                });
            }
        }
    }

    // Fall back to custom domain: <function>.<domain>
    if let Some(domain) = domain {
        if !domain.is_empty() {
            let suffix = format!(".{}", domain);
            if let Some(function_name) = host_without_port.strip_suffix(&suffix) {
                if !function_name.is_empty() {
                    return Some(VirtualHostInfo {
                        function_name,
                        region: None,
                    });
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- AWS-style tests ----

    #[test]
    fn aws_style_basic() {
        let info =
            extract_function_from_host("my-func.lambda.us-east-1.amazonaws.com", None).unwrap();
        assert_eq!(info.function_name, "my-func");
        assert_eq!(info.region, Some("us-east-1"));
    }

    #[test]
    fn aws_style_with_port() {
        let info =
            extract_function_from_host("my-func.lambda.us-east-1.amazonaws.com:9600", None)
                .unwrap();
        assert_eq!(info.function_name, "my-func");
        assert_eq!(info.region, Some("us-east-1"));
    }

    #[test]
    fn aws_style_different_region() {
        let info =
            extract_function_from_host("hello-world.lambda.eu-west-1.amazonaws.com", None)
                .unwrap();
        assert_eq!(info.function_name, "hello-world");
        assert_eq!(info.region, Some("eu-west-1"));
    }

    #[test]
    fn aws_style_underscores_in_name() {
        let info =
            extract_function_from_host("my_function.lambda.us-east-1.amazonaws.com", None)
                .unwrap();
        assert_eq!(info.function_name, "my_function");
        assert_eq!(info.region, Some("us-east-1"));
    }

    #[test]
    fn aws_style_missing_function_name() {
        // ".lambda.us-east-1.amazonaws.com" -> empty function name
        let result =
            extract_function_from_host(".lambda.us-east-1.amazonaws.com", None);
        assert!(result.is_none());
    }

    #[test]
    fn aws_style_missing_region() {
        // "my-func.lambda..amazonaws.com" -> empty region
        let result =
            extract_function_from_host("my-func.lambda..amazonaws.com", None);
        assert!(result.is_none());
    }

    #[test]
    fn aws_style_no_lambda_segment() {
        // "my-func.us-east-1.amazonaws.com" -> no .lambda. segment
        let result =
            extract_function_from_host("my-func.us-east-1.amazonaws.com", None);
        assert!(result.is_none());
    }

    // ---- Custom domain tests ----

    #[test]
    fn custom_domain_basic() {
        let info =
            extract_function_from_host("my-func.lambda.local", Some("lambda.local")).unwrap();
        assert_eq!(info.function_name, "my-func");
        assert!(info.region.is_none());
    }

    #[test]
    fn custom_domain_with_port() {
        let info =
            extract_function_from_host("my-func.lambda.local:9600", Some("lambda.local"))
                .unwrap();
        assert_eq!(info.function_name, "my-func");
        assert!(info.region.is_none());
    }

    #[test]
    fn custom_domain_empty_function() {
        let result =
            extract_function_from_host(".lambda.local", Some("lambda.local"));
        assert!(result.is_none());
    }

    #[test]
    fn custom_domain_no_match() {
        let result =
            extract_function_from_host("my-func.other.local", Some("lambda.local"));
        assert!(result.is_none());
    }

    #[test]
    fn custom_domain_none() {
        // No custom domain configured, no amazonaws.com suffix -> None
        let result = extract_function_from_host("my-func.lambda.local", None);
        assert!(result.is_none());
    }

    // ---- Priority tests ----

    #[test]
    fn aws_style_takes_precedence_over_custom_domain() {
        // Even with a custom domain set, AWS-style should match first
        let info = extract_function_from_host(
            "my-func.lambda.us-east-1.amazonaws.com",
            Some("amazonaws.com"),
        )
        .unwrap();
        assert_eq!(info.function_name, "my-func");
        assert_eq!(info.region, Some("us-east-1"));
    }

    // ---- No-match tests ----

    #[test]
    fn plain_localhost() {
        let result = extract_function_from_host("localhost", None);
        assert!(result.is_none());
    }

    #[test]
    fn plain_localhost_with_port() {
        let result = extract_function_from_host("localhost:9600", None);
        assert!(result.is_none());
    }

    #[test]
    fn ip_address() {
        let result = extract_function_from_host("127.0.0.1:9600", None);
        assert!(result.is_none());
    }
}
