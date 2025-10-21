"""
URL normalization and canonicalization.

Implements the normalization strategy from spec.md §1.1:
- Parse with robust library, strip fragments
- Lowercase scheme/host, percent-decode minimally
- Extract eTLD+1 via Public Suffix List
- Convert to punycode
"""

from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from publicsuffixlist import PublicSuffixList


@dataclass(frozen=True)
class NormalizedURL:
    """
    Normalized URL components.

    Attributes:
        scheme: Normalized scheme (lowercase)
        host: Normalized host (lowercase, punycode)
        port: Port number (None if default for scheme)
        path: Normalized path
        query: Normalized query string
        domain: eTLD+1 extracted domain
        raw: Original raw URL
    """

    scheme: str
    host: str
    port: Optional[int]
    path: str
    query: str
    domain: str
    raw: str

    def to_url(self) -> str:
        """Reconstruct normalized URL string (without fragment)."""
        netloc = self.host
        if self.port is not None:
            netloc = f"{netloc}:{self.port}"

        return urlunparse((self.scheme, netloc, self.path, "", self.query, ""))

    def get_path_query(self) -> str:
        """Get combined path+query for storage."""
        if self.query:
            return f"{self.path}?{self.query}"
        return self.path


class URLNormalizer:
    """
    URL normalization and canonicalization engine.

    Usage:
        normalizer = URLNormalizer()
        result = normalizer.normalize("https://Example.COM:443/path/../foo?b=2&a=1#fragment")
        print(result.to_url())  # https://example.com/foo?a=1&b=2
        print(result.domain)     # example.com
    """

    # Default ports for common schemes
    DEFAULT_PORTS = {
        "http": 80,
        "https": 443,
        "ftp": 21,
        "ftps": 990,
    }

    def __init__(self):
        """Initialize normalizer with Public Suffix List."""
        self.psl = PublicSuffixList()

    def normalize(self, url: str) -> NormalizedURL:
        """
        Normalize a URL according to spec.md §1.1.

        Args:
            url: Raw URL string

        Returns:
            NormalizedURL with all components normalized

        Raises:
            ValueError: If URL cannot be parsed
        """
        if not url or not isinstance(url, str):
            raise ValueError(f"Invalid URL: {url}")

        # Parse URL
        try:
            parsed = urlparse(url.strip())
        except Exception as e:
            raise ValueError(f"Failed to parse URL '{url}': {e}")

        # Extract and normalize components
        scheme = self._normalize_scheme(parsed.scheme)
        host = self._normalize_host(parsed.hostname)
        port = self._normalize_port(parsed.port, scheme)
        path = self._normalize_path(parsed.path)
        query = self._normalize_query(parsed.query)

        # Extract eTLD+1 domain
        domain = self._extract_domain(host)

        return NormalizedURL(
            scheme=scheme,
            host=host,
            port=port,
            path=path,
            query=query,
            domain=domain,
            raw=url,
        )

    def _normalize_scheme(self, scheme: Optional[str]) -> str:
        """Normalize scheme to lowercase, default to http."""
        if not scheme:
            return "http"
        return scheme.lower()

    def _normalize_host(self, host: Optional[str]) -> str:
        """
        Normalize host: lowercase and convert to punycode if needed.

        Args:
            host: Hostname from parsed URL

        Returns:
            Normalized host in ASCII (punycode for IDN)
        """
        if not host:
            raise ValueError("URL must have a host")

        # Lowercase
        host = host.lower()

        # Convert to punycode (idna encoding) if needed
        try:
            # This handles internationalized domain names
            host = host.encode("idna").decode("ascii")
        except (UnicodeError, UnicodeDecodeError):
            # Already ASCII or invalid, keep as-is
            pass

        return host

    def _normalize_port(self, port: Optional[int], scheme: str) -> Optional[int]:
        """
        Normalize port: return None if it's the default port for the scheme.

        Args:
            port: Port number from parsed URL
            scheme: Normalized scheme

        Returns:
            Port number or None if default
        """
        if port is None:
            return None

        # Drop port if it's the default for this scheme
        if port == self.DEFAULT_PORTS.get(scheme):
            return None

        return port

    def _normalize_path(self, path: str) -> str:
        """
        Normalize path: resolve ./ and ../, collapse multiple slashes.

        Args:
            path: Path from parsed URL

        Returns:
            Normalized path
        """
        if not path:
            return "/"

        # Split into segments
        segments = path.split("/")

        # Resolve . and ..
        normalized = []
        for segment in segments:
            if segment == "." or segment == "":
                # Skip current directory and empty segments (but keep leading /)
                if not normalized:  # Keep leading slash
                    normalized.append("")
                continue
            elif segment == "..":
                # Go up one level
                if len(normalized) > 1:  # Don't go above root
                    normalized.pop()
            else:
                normalized.append(segment)

        # Rejoin
        result = "/".join(normalized)

        # Ensure path starts with /
        if not result.startswith("/"):
            result = "/" + result

        # Preserve trailing slash from original if path had segments
        if path.endswith("/") and not result.endswith("/") and len(normalized) > 1:
            result += "/"

        return result

    def _normalize_query(self, query: str) -> str:
        """
        Normalize query string: sort parameters by key.

        Preserves duplicate keys in sorted order.

        Args:
            query: Query string from parsed URL

        Returns:
            Normalized query string (sorted by key)
        """
        if not query:
            return ""

        # Parse query parameters
        params = parse_qs(query, keep_blank_values=True)

        # Sort by key, preserve duplicate values
        sorted_params = []
        for key in sorted(params.keys()):
            for value in params[key]:
                sorted_params.append((key, value))

        # Re-encode
        return urlencode(sorted_params)

    def _extract_domain(self, host: str) -> str:
        """
        Extract eTLD+1 (effective top-level domain + 1 label) using Public Suffix List.

        Args:
            host: Normalized hostname

        Returns:
            eTLD+1 domain (e.g., 'example.com' from 'www.example.com')
        """
        try:
            # Get the private domain (eTLD+1)
            domain = self.psl.privatesuffix(host)
            if domain:
                return domain

            # Fallback: if PSL doesn't find a suffix, return the host
            # This handles cases like 'localhost' or IP addresses
            return host
        except Exception:
            # Fallback to host if extraction fails
            return host
