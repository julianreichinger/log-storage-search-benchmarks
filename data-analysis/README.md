# Data Analysis

## How to build a runnable JAR

From the repo root directory, run:
> ./gradlew :data-analysis:shadowJar

The standalone JAR file can be found under:
> project_root/data-analysis/build/libs/data-analysis-all.jar

Run the application with:
> java -jar data-analysis-all.jar {command} {option=value}*

## Commands (and classes)
See command classes for descriptions and available parameters.

* "analyzeGroups" (GroupAnalyzer)
* "analyzeTokens" (TokenAnalyzer)
* "generate" (LogGenerator)
* "shuffle" (LineShuffler)