from urllib.parse import urlparse


def is_valid_url(url: str) -> bool:
    """Check if a URL is valid

    Args:
        url (str): The URL to check.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    try:
        result = urlparse(url)
        return all([result.scheme in ["http", "https"], result.netloc])
    except ValueError:
        return False
