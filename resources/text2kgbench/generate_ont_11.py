import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Airport",
        "dbo:Anthem",
        "dbo:BodyStyle",
        "dbo:Demonym",
        "dbo:Division",
        "dbo:EthnicGroup",
        "dbo:Function",
        "dbo:Language",
        "dbo:Organisation",
        "dbo:PowerType",
        "dbo:Product",
        "dbo:Saint",
        "dbo:ShipClass",
        "dbo:Wheelbase",
        "dbo:Flight",
        "dbo:Ship",
        "dbo:City",
        "dbo:Place",
        "dbo:Country",
        "dbo:MeanOfTransportation"
    ]

    properties = [
        "dbo:activeYearsStartDate",
        "dbo:alternativeName",
        "dbo:anthem",
        "dbo:areaTotal",
        "dbo:assembly",
        "dbo:bodyStyle",
        "dbo:buildDate",
        "dbo:builder",
        "dbo:capital",
        "dbo:christeningDate",
        "dbo:city",
        "dbo:class",
        "dbo:comparable",
        "dbo:completionDate",
        "dbo:country",
        "dbo:countryOrigin",
        "dbo:cylinderCount",
        "dbo:demonym",
        "dbo:designCompany",
        "dbo:diameter",
        "dbo:division",
        "dbo:engine",
        "dbo:ethnicGroup",
        "dbo:extinctionDate",
        "dbo:failedLaunches",
        "dbo:fate",
        "dbo:finalFlight",
        "dbo:foundationPlace",
        "dbo:foundedBy",
        "dbo:function",
        "dbo:headquarter",
        "dbo:isPartOf",
        "dbo:keyPerson",
        "dbo:language",
        "dbo:launchSite",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:length",
        "dbo:location",
        "dbo:maidenFlight",
        "dbo:maidenVoyage",
        "dbo:manufacturer",
        "dbo:modelYears",
        "dbo:operator",
        "dbo:owner",
        "dbo:parentCompany",
        "dbo:powerType",
        "dbo:product",
        "dbo:productionEndYear",
        "dbo:productionStartYear",
        "dbo:relatedMeanOfTransportation",
        "dbo:rocketStages",
        "dbo:saint",
        "dbo:shipBeam",
        "dbo:shipClass",
        "dbo:shipDisplacement",
        "dbo:shipDraft",
        "dbo:shipLaunch",
        "dbo:shipOrdered",
        "dbo:site",
        "dbo:status",
        "dbo:subsidiary",
        "dbo:successor",
        "dbo:topSpeed",
        "dbo:totalLaunches",
        "dbo:totalProduction",
        "dbo:transmission",
        "dbo:wheelbase"
    ]

    generate(classes, properties, "ont_11", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])