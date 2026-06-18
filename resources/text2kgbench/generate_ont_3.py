import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Battle",
        "dbo:Class",
        "dbo:Country",
        "dbo:County",
        "dbo:Demonym",
        "dbo:Division",
        "dbo:Party",
        "dbo:Language",
        "dbo:Place",
        "dbo:Aircraft",
        "dbo:RunwaySurfaceType",
        "dbo:City",
        "dbo:Airport"
    ]

    properties = [
        "dbo:1stRunwaySurfaceType",
        "dbo:2ndRunwaySurfaceType",
        "dbo:3rdRunwaySurfaceType",
        "dbo:aircraftFighter",
        "dbo:aircraftHelicopter",
        "dbo:areaCode",
        "dbo:battle",
        "dbo:capital",
        "dbo:ceremonialCounty",
        "dbo:city",
        "dbo:cityServed",
        "dbo:class",
        "dbo:country",
        "dbo:demonym",
        "dbo:division",
        "dbo:elevationAboveTheSeaLevel",
        "dbo:elevationAboveTheSeaLevelInMetres",
        "dbo:foundedBy",
        "dbo:foundingYear",
        "dbo:headquarter",
        "dbo:hubAirport",
        "dbo:icaoLocationIdentifier",
        "dbo:isPartOf",
        "dbo:language",
        "dbo:largestCity",
        "dbo:leader",
        "dbo:leaderParty",
        "dbo:leaderTitle",
        "dbo:location",
        "dbo:officialLanguage",
        "dbo:operatingOrganisation",
        "dbo:order",
        "dbo:owner",
        "dbo:postalCode",
        "dbo:regionServed",
        "dbo:runwayLength",
        "dbo:runwayName",
        "dbo:runwaySurfaceType",
        "dbo:transportAircraft"
    ]

    generate(classes, properties, "ont_3", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])