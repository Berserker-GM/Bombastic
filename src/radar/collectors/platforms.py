"""Platform labels shared by contest collectors."""


def platform_slug(host: str) -> str:
    """Return the platform label for a host, such as codechef.com -> codechef."""
    normalized = host.strip().casefold()
    if normalized.startswith("www."):
        normalized = normalized.removeprefix("www.")
    slug, separator, rest = normalized.partition(".")
    if not slug or (separator and not rest):
        raise ValueError("host has no platform slug")
    return slug
