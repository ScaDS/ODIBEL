import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Anthem",
        "dbo:Background",
        "dbo:Currency",
        "dbo:Demonym",
        "dbo:EthnicGroup",
        "dbo:Field",
        "dbo:Instrument",
        "dbo:Occupation",
        "dbo:RecordLabel",
        "dbo:Training",
        "dbo:Band",
        "dbo:Company",
        "dbo:Language",
        "dbo:Genre",
        "dbo:Place",
        "dbo:City",
        "dbo:MusicalWork",
        "dbo:Country",
        "dbo:Artist"
    ]

    properties = [
        "dbo:activeYearsStartYear",
        "dbo:alternativeName",
        "dbo:anthem",
        "dbo:areaTotal",
        "dbo:associatedBand/associatedMusicalArtist",
        "dbo:background",
        "dbo:birthDate",
        "dbo:birthPlace",
        "dbo:birthYear",
        "dbo:country",
        "dbo:currency",
        "dbo:deathDate",
        "dbo:deathPlace",
        "dbo:demonym",
        "dbo:derivative",
        "dbo:elevationAboveTheSeaLevel",
        "dbo:ethnicGroup",
        "dbo:foundingDate",
        "dbo:genre",
        "dbo:instrument",
        "dbo:isPartOf",
        "dbo:language",
        "dbo:leader",
        "dbo:leaderTitle",
        "dbo:location",
        "dbo:longName",
        "dbo:meaning",
        "dbo:musicFusionGenre",
        "dbo:musicSubgenre",
        "dbo:nationality",
        "dbo:occupation",
        "dbo:officialLanguage",
        "dbo:origin",
        "dbo:populationDensity",
        "dbo:postalCode",
        "dbo:professionalField",
        "dbo:recordLabel",
        "dbo:stylisticOrigin",
        "dbo:training"
    ]

    generate(classes, properties, "ont_17", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])