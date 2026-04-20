from enum import StrEnum


class StoryTag(StrEnum):
    BIZJET = "bizjet"
    CARGO = "cargo"
    COMMERCIAL = "commercial"
    EMERGENCY = "emergency"
    GLIDER = "glider"
    GOVERNMENT = "government"
    HELICOPTER = "helicopter"
    HISTORIC = "historic"
    LONG_HAUL = "long-haul"
    LOW_COST = "low-cost"
    MEDICAL = "medical"
    MILITARY = "military"
    POLICE = "police"
    REGIONAL = "regional"
    RESEARCH = "research"
    SPECIAL_MISSION = "special-mission"
    TURBOPROP = "turboprop"
    UNUSUAL_OPERATOR = "unusual-operator"
    VIP = "vip"
    WIDEBODY = "widebody"


TAG_DESCRIPTIONS: dict[StoryTag, str] = {
    StoryTag.BIZJET: "business jets and private jets",
    StoryTag.CARGO: "freight and cargo aircraft",
    StoryTag.COMMERCIAL: "routine airline traffic",
    StoryTag.EMERGENCY: "squawk 7700, 7600, or 7500",
    StoryTag.GLIDER: "sailplanes and motor gliders",
    StoryTag.GOVERNMENT: "state or government flights (e.g. Flugbereitschaft)",
    StoryTag.HELICOPTER: "rotary-wing aircraft",
    StoryTag.HISTORIC: "vintage or classic aircraft",
    StoryTag.LONG_HAUL: "intercontinental routes",
    StoryTag.LOW_COST: "budget airlines (Ryanair, Wizz, etc.)",
    StoryTag.MEDICAL: "air ambulance, medevac, hospital transport, organ flights",
    StoryTag.MILITARY: "any military aircraft",
    StoryTag.POLICE: "law enforcement, border control, customs aviation",
    StoryTag.REGIONAL: "regional jets and regional airliners",
    StoryTag.RESEARCH: "scientific or test aircraft (DLR, NLR, flight test)",
    StoryTag.SPECIAL_MISSION: "ELINT, AEW&C, surveillance, calibration",
    StoryTag.TURBOPROP: "turboprop aircraft",
    StoryTag.UNUSUAL_OPERATOR: "rare or exotic operator for central Europe",
    StoryTag.VIP: "VIP transport (heads of state, royal flights)",
    StoryTag.WIDEBODY: "wide-body aircraft (A380, 777, A350, etc.)",
}

assert set(TAG_DESCRIPTIONS) == set(StoryTag), (
    f"TAG_DESCRIPTIONS mismatch: missing {set(StoryTag) - set(TAG_DESCRIPTIONS)}"
)
