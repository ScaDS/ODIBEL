import sys

from generate import generate

def run(input_path, output_path, ont_path):

    classes = [
        "dbo:Cause",
        "dbo:Country",
        "dbo:Periapsis",
        "dbo:School",
        "dbo:State",
        "dbo:Place",
        "dbo:Person",
        "dbo:CelestialBody"
    ]

    properties = [
        "dbo:absoluteMagnitude",
        "dbo:almaMater",
        "dbo:apoapsis",
        "dbo:averageSpeed",
        "dbo:birthDate",
        "dbo:birthPlace",
        "dbo:deathCause",
        "dbo:deathDate",
        "dbo:deathPlace",
        "dbo:density",
        "dbo:discovered",
        "dbo:discoverer",
        "dbo:doctoralStudent",
        "dbo:epoch",
        "dbo:escapeVelocity",
        "dbo:formerName",
        "dbo:mass",
        "dbo:maximumTemperature",
        "dbo:meanTemperature",
        "dbo:minimumTemperature",
        "dbo:nationality",
        "dbo:orbitalPeriod",
        "dbo:periapsis",
        "dbo:rotationPeriod",
        "dbo:stateOfOrigin",
        "dbo:surfaceArea",
        "dbo:temperature"
    ]

    generate(classes, properties, "ont_8", input_path, output_path, ont_path)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: test.py <input_dir> <output_path> <ontology_path>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])