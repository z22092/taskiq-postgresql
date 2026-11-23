import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Any role with CONNECT can NOTIFY the channel, and a payload can hold close to
# 8 KB, so a log line must not carry it whole.
_MAX_LOGGED_PAYLOAD = 64


def parse_message_id(payload: str, channel_name: str) -> Optional[int]:
    """
    Read the message id a ``NOTIFY`` carries.

    Every listen driver goes through here so a bad payload is handled the same
    way: logged with a bounded excerpt and skipped, instead of ending the
    listener or filling the log with attacker-sized text.

    Args:
        payload: the notification payload.
        channel_name: the channel it arrived on, for the log line.

    Returns:
        The id, or ``None`` when the payload is not an integer.
    """
    try:
        return int(payload)
    except ValueError:
        excerpt = repr(payload[:_MAX_LOGGED_PAYLOAD])
        if len(payload) > _MAX_LOGGED_PAYLOAD:
            excerpt += f" (truncated, {len(payload)} characters)"
        logger.warning(
            "Invalid NOTIFY payload %s on channel %s",
            excerpt,
            channel_name,
        )
        return None
