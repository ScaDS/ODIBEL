import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Album",
        "dbo:Band",
        "dbo:Certification",
        "dbo:Company",
        "dbo:Format",
        "dbo:Instrument",
        "dbo:Language",
        "dbo:MusicType",
        "dbo:Organisation",
        "dbo:Timezone",
        "dbo:Place",
        "dbo:RecordLabel",
        "dbo:Genre",
        "dbo:Person",
        "dbo:MusicalWork"
    ]

    properties = [
        "dbo:album",
        "dbo:areaCode",
        "dbo:areaTotal",
        "dbo:artist",
        "dbo:associatedBand/associatedMusicalArtist",
        "dbo:birthDate",
        "dbo:certification",
        "dbo:derivative",
        "dbo:distributingLabel",
        "dbo:followedBy",
        "dbo:format",
        "dbo:formerBandMember",
        "dbo:genre",
        "dbo:instrument",
        "dbo:keyPerson",
        "dbo:language",
        "dbo:leader",
        "dbo:location",
        "dbo:musicFusionGenre",
        "dbo:musicSubgenre",
        "dbo:musicalArtist",
        "dbo:musicalBand",
        "dbo:owner",
        "dbo:parentCompany",
        "dbo:precededBy",
        "dbo:producer",
        "dbo:recordLabel",
        "dbo:recordedIn",
        "dbo:releaseDate",
        "dbo:runtime",
        "dbo:stylisticOrigin",
        "dbo:timeZone",
        "dbo:type",
        "dbo:utcOffset",
        "dbo:writer"
    ]

    generate(classes, properties, "ont_2", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])