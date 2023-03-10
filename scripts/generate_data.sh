#!/bin/bash -e

./gradlew :data-analysis:shadowJar

mkdir -p data

java -jar data-analysis/build/libs/data-analysis-all.jar generate distributionIn=data-analysis/src/main/resources/distribution/1M_production_distribution groups=3233 in=input out=data/1M_generated
java -jar data-analysis/build/libs/data-analysis-all.jar generate distributionIn=data-analysis/src/main/resources/distribution/5M_production_distribution groups=60579 in=input out=data/5M_generated