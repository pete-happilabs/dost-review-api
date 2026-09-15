"""Three id forms coexist. This module names them so nothing else has to guess."""

WIRE_PREFIXES = ("hum.", "agent.", "com.")


def is_wire_id(value: str) -> bool:
    """A DES chat-wire entity id (Onboard profile.dasListingId or an agent address)."""
    return isinstance(value, str) and value.startswith(WIRE_PREFIXES)


def is_profile_uuid(value: str) -> bool:
    return isinstance(value, str) and len(value) == 36 and value.count("-") == 4
