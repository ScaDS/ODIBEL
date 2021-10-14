#!/usr/bin/env bash

mvn -q scala:run -DmainClass="odibel.Main" -DaddArgs="generate-completion" > bin/_odibel_completion.sh
