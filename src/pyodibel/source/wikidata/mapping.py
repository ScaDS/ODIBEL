"""
Entity mapping between source formats and ODIBEL internal representation.

This module will provide mappings between:
- WikidataEntity (from Wikidata dumps)
- OdibelEntity (internal ODIBEL entity representation)

TODO: Implement OdibelEntity model and mapping functions.
"""

from typing import Dict, Any, Optional
from pyodibel.source.wikidata.entity import WikidataEntity, ExternalIDClaim, WikidataClaim

# TODO: Define OdibelEntity model
# class OdibelEntity(BaseModel):
#     """Internal ODIBEL entity representation."""
#     pass


def map_wikidata_to_odibel(wikidata_entity: WikidataEntity) -> Dict[str, Any]:
    """
    Map WikidataEntity to OdibelEntity format.
    
    This function will convert Wikidata-specific structures to
    the internal ODIBEL entity representation.
    
    Args:
        wikidata_entity: WikidataEntity to map
        
    Returns:
        Dictionary representation of OdibelEntity (or OdibelEntity when implemented)
    
    TODO: Implement full mapping logic
    """
    # Placeholder implementation
    return {
        "id": wikidata_entity.id,
        "type": wikidata_entity.type,
        "labels": {
            lang: label.value
            for lang, label in (wikidata_entity.labels or {}).items()
        },
        # TODO: Map claims, descriptions, etc. to OdibelEntity structure
    }


def map_external_ids(entity: WikidataEntity):
    """
    Extract external ID claims from a Wikidata entity.
    
    Args:
        entity: WikidataEntity to extract external IDs from
        
    Returns:
        List of ExternalIDClaim objects
    """
    results = []
    try:
        if not entity.claims:
            return results

        type_claims = entity.get_claims("P31")
        types = []
        for type_claim_dict in type_claims:
            type_claim = WikidataClaim(**type_claim_dict)
            if type_claim.mainsnak and type_claim.mainsnak.datatype == "wikibase-item":
                datavalue = type_claim.mainsnak.datavalue
                if datavalue and isinstance(datavalue, dict):
                    # Get the actual value from the datavalue dict
                    value = datavalue.get("value", "")
                    # If value is still a dict (nested), try to extract string representation
                    if isinstance(value, dict):
                        value = str(value["id"])
                    else:
                        value = str(value)
                else:
                    value = str(datavalue) if datavalue else ""
                types.append(value)
            else:
                types.append("item")
        if not types:
            types.append("item")

        sorted_types = sorted(types)
            
        for property_key, property_claims in entity.claims.items():
            for claim_dict in property_claims:
                claim = WikidataClaim(**claim_dict)
                if claim.mainsnak and claim.mainsnak.datatype == "external-id":
                    # Extract value from datavalue dict
                    # datavalue structure: {"type": "string", "value": "actual_value"}
                    datavalue = claim.mainsnak.datavalue
                    if datavalue and isinstance(datavalue, dict):
                        # Get the actual value from the datavalue dict
                        value = datavalue.get("value", "")
                        # If value is still a dict (nested), try to extract string representation
                        if isinstance(value, dict):
                            value = str(value)
                        else:
                            value = str(value)
                    else:
                        value = str(datavalue) if datavalue else ""
                    
                    results.append(ExternalIDClaim(
                        id=entity.id,
                        type='|'.join(sorted_types),
                        property=property_key,
                        value=value
                    ))
    except Exception as e:
        # Skip entities with errors
        print(f"Error extracting external IDs from entity {entity.id}: {e}")
        pass        
    return results