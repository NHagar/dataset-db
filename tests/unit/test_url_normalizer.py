"""Unit tests for URL normalization."""

import pytest

from dataset_db.normalization import NormalizedURL, URLNormalizer


class TestURLNormalizer:
    """Test suite for URLNormalizer."""

    @pytest.fixture
    def normalizer(self):
        """Create a URLNormalizer instance."""
        return URLNormalizer()

    def test_basic_normalization(self, normalizer):
        """Test basic URL normalization."""
        result = normalizer.normalize("https://example.com/path")

        assert result.scheme == "https"
        assert result.host == "example.com"
        assert result.port is None
        assert result.path == "/path"
        assert result.query == ""
        assert result.domain == "example.com"
        assert result.raw == "https://example.com/path"

    def test_scheme_normalization(self, normalizer):
        """Test scheme is normalized to lowercase."""
        result = normalizer.normalize("HTTPS://Example.COM/")
        assert result.scheme == "https"

    def test_default_scheme(self, normalizer):
        """Test default scheme is http."""
        result = normalizer.normalize("//example.com/path")
        assert result.scheme == "http"

    def test_host_normalization(self, normalizer):
        """Test host is normalized to lowercase."""
        result = normalizer.normalize("https://Example.COM/path")
        assert result.host == "example.com"

    def test_default_port_removal(self, normalizer):
        """Test default ports are removed."""
        # HTTPS default port 443
        result = normalizer.normalize("https://example.com:443/path")
        assert result.port is None

        # HTTP default port 80
        result = normalizer.normalize("http://example.com:80/path")
        assert result.port is None

    def test_non_default_port(self, normalizer):
        """Test non-default ports are preserved."""
        result = normalizer.normalize("https://example.com:8443/path")
        assert result.port == 8443

    def test_path_normalization(self, normalizer):
        """Test path normalization."""
        # Resolve ..
        result = normalizer.normalize("https://example.com/a/b/../c")
        assert result.path == "/a/c"

        # Remove .
        result = normalizer.normalize("https://example.com/a/./b")
        assert result.path == "/a/b"

        # Collapse multiple slashes
        result = normalizer.normalize("https://example.com/a//b")
        assert result.path == "/a/b"

    def test_trailing_slash_preserved(self, normalizer):
        """Test trailing slash is preserved."""
        result = normalizer.normalize("https://example.com/path/")
        assert result.path == "/path/"

    def test_query_normalization(self, normalizer):
        """Test query parameters are sorted."""
        result = normalizer.normalize("https://example.com/path?z=1&a=2&m=3")
        assert result.query == "a=2&m=3&z=1"

    def test_query_duplicate_keys(self, normalizer):
        """Test duplicate query keys are preserved."""
        result = normalizer.normalize("https://example.com/?a=1&b=2&a=3")
        # Should preserve both 'a' values, sorted
        assert "a=1" in result.query
        assert "a=3" in result.query
        assert "b=2" in result.query

    def test_fragment_removal(self, normalizer):
        """Test URL fragments are removed."""
        result = normalizer.normalize("https://example.com/path#fragment")
        assert result.path == "/path"
        assert "#" not in result.to_url()

    def test_etld_extraction(self, normalizer):
        """Test eTLD+1 extraction."""
        # Subdomain should be stripped
        result = normalizer.normalize("https://www.example.com/path")
        assert result.domain == "example.com"

        # co.uk should be recognized as public suffix
        result = normalizer.normalize("https://www.example.co.uk/path")
        assert result.domain == "example.co.uk"

    def test_punycode_conversion(self, normalizer):
        """Test internationalized domain names are converted to punycode."""
        # Chinese domain
        result = normalizer.normalize("https://中国.example.com/path")
        assert result.host.startswith("xn--")
        assert result.host.endswith(".example.com")

    def test_to_url(self, normalizer):
        """Test reconstructing normalized URL."""
        result = normalizer.normalize(
            "HTTPS://Example.COM:443/a/../b?z=1&a=2#fragment"
        )
        url = result.to_url()

        assert url == "https://example.com/b?a=2&z=1"
        assert "#" not in url

    def test_get_path_query(self, normalizer):
        """Test getting combined path+query."""
        result = normalizer.normalize("https://example.com/path?a=1")
        assert result.get_path_query() == "/path?a=1"

        # Without query
        result = normalizer.normalize("https://example.com/path")
        assert result.get_path_query() == "/path"

    def test_invalid_url(self, normalizer):
        """Test invalid URLs raise ValueError."""
        with pytest.raises(ValueError):
            normalizer.normalize("")

        with pytest.raises(ValueError):
            normalizer.normalize(None)

    def test_url_without_host(self, normalizer):
        """Test URL without host raises ValueError."""
        with pytest.raises(ValueError):
            normalizer.normalize("https:///path")

    def test_complex_real_world_url(self, normalizer):
        """Test complex real-world URL."""
        url = "HTTPS://WWW.Example.COM:443/Path/To/../Resource?utm_source=test&id=123&utm_source=other#section"
        result = normalizer.normalize(url)

        assert result.scheme == "https"
        assert result.host == "www.example.com"
        assert result.port is None
        assert result.path == "/Path/Resource"
        assert "id=123" in result.query
        assert "utm_source=test" in result.query
        assert "utm_source=other" in result.query
        assert result.domain == "example.com"


class TestNormalizedURL:
    """Test NormalizedURL dataclass."""

    def test_immutability(self):
        """Test NormalizedURL is immutable (frozen)."""
        url = NormalizedURL(
            scheme="https",
            host="example.com",
            port=None,
            path="/path",
            query="a=1",
            domain="example.com",
            raw="https://example.com/path?a=1",
        )

        with pytest.raises(Exception):  # FrozenInstanceError
            url.scheme = "http"
