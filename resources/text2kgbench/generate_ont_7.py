import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:CompanyType",
        "dbo:EthnicGroup",
        "dbo:Industry",
        "dbo:Product",
        "dbo:Service",
        "dbo:City",
        "dbo:Country",
        "dbo:Person",
        "dbo:Place",
        "dbo:Company"
    ]

    properties = [
        "dbo:areaTotal",
        "dbo:capital",
        "dbo:city",
        "dbo:country",
        "dbo:elevationAboveTheSeaLevel",
        "dbo:ethnicGroup",
        "dbo:foundationPlace",
        "dbo:foundingDate",
        "dbo:industry",
        "dbo:isPartOf",
        "dbo:keyPerson",
        "dbo:leader",
        "dbo:leaderParty",
        "dbo:leaderTitle",
        "dbo:location",
        "dbo:longName",
        "dbo:netIncome",
        "dbo:numberOfEmployees",
        "dbo:numberOfLocations",
        "dbo:operatingIncome",
        "dbo:parentCompany",
        "dbo:populationTotal",
        "dbo:product",
        "dbo:regionServed",
        "dbo:revenue",
        "dbo:service",
        "dbo:subsidiary",
        "dbo:type"
    ]

    generate(classes, properties, "ont_7", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])