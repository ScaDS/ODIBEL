import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:EthnicGroup",
        "dbo:ArchitecturalStyle",
        "dbo:Architecture",
        "dbo:City",
        "dbo:Currency",
        "dbo:Demonym",
        "dbo:Language",
        "dbo:State",
        "dbo:Country",
        "dbo:Organisation",
        "dbo:Tenant",
        "dbo:Person",
        "dbo:Place",
        "dbo:Building"
    ]

    properties = [
        "dbo:NationalRegisterOfHistoricPlacesReferenceNumber",
        "dbo:addedToTheNationalRegisterOfHistoricPlaces",
        "dbo:address",
        "dbo:architect",
        "dbo:architecturalStyle",
        "dbo:architecture",
        "dbo:bedCount",
        "dbo:birthPlace",
        "dbo:buildingStartDate",
        "dbo:capital",
        "dbo:chancellor",
        "dbo:completionDate",
        "dbo:cost",
        "dbo:country",
        "dbo:currency",
        "dbo:currentTenants",
        "dbo:deathPlace",
        "dbo:demonym",
        "dbo:ethnicGroup",
        "dbo:floorArea",
        "dbo:floorCount",
        "dbo:foundationPlace",
        "dbo:governingBody",
        "dbo:height",
        "dbo:inaugurationDate",
        "dbo:isPartOf",
        "dbo:keyPerson",
        "dbo:language",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:location",
        "dbo:origin",
        "dbo:owner",
        "dbo:region",
        "dbo:significantBuilding",
        "dbo:state",
        "dbo:tenant",
        "dbo:yearOfConstruction"
    ]

    generate(classes, properties, "ont_4", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])