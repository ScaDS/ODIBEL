import jpype
import jpype.imports
from jpype.types import *

# Start JVM
jpype.startJVM(classpath=['HelloWorld.jar'])

# Import the Scala class
from example import HelloWorld

# Create an instance of the class
hw = HelloWorld()

# Call the method
result = hw.greet("Alice")
print(result)  # Expected: Hello, Alice from Scala!

# Shutdown JVM
jpype.shutdownJVM()
