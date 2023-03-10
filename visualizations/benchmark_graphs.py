import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os

def extractResults(inputDir, filterSpec, groupSpec, barSpec, stackSpecs):

    resultFiles = [f for f in os.listdir(inputDir) if os.path.isfile(os.path.join(inputDir, f))]

    # Load JMH json results
    resultJsons = []
    for result in resultFiles:
        with open(os.path.join(inputDir, result), "r") as f:
            results = json.load(f)
            resultJsons.extend(results)

    groupKeys = groupSpec[0]
    barKeys = barSpec[0]

    # Extract results from the JMH jsons
    resultMap = {}
    for result in resultJsons:
        if (getJsonValue(filterSpec[0], result) != filterSpec[1]):
            continue

        group = getLastPart(getJsonValue(groupKeys, result))
        bar = getLastPart(getJsonValue(barKeys, result))
        for stackSpec in stackSpecs:
            stack = stackSpec[1]
            stackValue = getJsonValue(stackSpec[0], result)

            stackNumber = 0
            if (isinstance(stackValue, list)):
                stackList = np.array(stackValue, dtype=np.float32)
                stackNumber = stackList.mean()
            else:
                stackNumber = float(stackValue)

            resultMap \
                .setdefault(group, {}) \
                .setdefault(bar, {}) \
                [stack] = stackNumber

    # Transform into the format needed for charting
    if len(groupSpec) == 1:
        groups = sorted(resultMap)
    else:
        groups = groupSpec[1]

    if len(barSpec) == 1:
        bars = sorted(resultMap[groups[0]])
    else:
        bars = barSpec[1]

    stacks = [spec[1] for spec in stackSpecs]

    result = np.zeros((len(groups), len(bars), len(stacks)))
    for groupIdx in range(len(groups)):
        groupEntry = resultMap[groups[groupIdx]]
        for barIdx in range(len(bars)):
            barEntry = groupEntry[bars[barIdx]]
            for stackIdx in range(len(stacks)):
                result[groupIdx, barIdx, stackIdx] = barEntry[stacks[stackIdx]]

    return result, groups, bars, stacks

def getJsonValue(keyParts, json):
    result = json
    for key in keyParts:
        result = result[key]
    return result

def getLastPart(string):
    parts = string.split('/')
    return parts[-1]


def createBarChart(results, groups, bars, stacks, unit, output, yscale, colors):
    _, ax = plt.subplots(layout='constrained')
    width = 0.95
    totalBars = (len(bars) + 1) * len(groups) - 1
    offsets = np.arange(totalBars)
    measurements = np.zeros(totalBars)
    bottom = np.zeros(totalBars)
    for stackIdx in range(len(stacks)):
        # Collect data for current stack layer
        measurementOffset = 0
        for groupIdx in range(len(groups)):
            measurements[measurementOffset:(measurementOffset+len(bars))] = results[groupIdx, :, stackIdx]
            measurementOffset += len(bars) + 1
        # Chart stack layer
        container = ax.bar(offsets, measurements, width, label=stacks[stackIdx], bottom=bottom, color=colors[stackIdx])
        # Prepare for next layer
        bottom = bottom + measurements
        # Label the bars with the total value
        if (stackIdx == len(stacks) - 1):
            if (yscale == "log"):
                barLabels = ["{:.1e}".format(height) if height > 0 else '' for height in bottom]
            else:
                barLabels = ["%.1f" % height if height > 0 else '' for height in bottom]
            ax.bar_label(container=container, padding=3, labels=barLabels, rotation=90)

    maxBarHeight = np.max(bottom)

    # Set bar axis labels
    xLabels = []
    for i in range(len(groups)):
        xLabels.extend(bars)
        xLabels.append("")
    xLabels.pop()
    ax.set_xticks(offsets, xLabels, rotation=90)

    artists = []
    # Set group axis labels
    if (len(groups) > 1):
        barWidth = 1 / totalBars
        groupOffset = (barWidth * len(bars)) / 2
        for group in groups:
            text = ax.text(groupOffset, -0.25, group, horizontalalignment="center", verticalalignment="center", transform=ax.transAxes)
            artists.append(text)
            groupOffset += (barWidth * (len(bars) + 1))

    if (len(stacks) > 1):
        legend = ax.legend(loc='upper center', ncol=len(stacks), bbox_to_anchor=(0.5, 1.1))
        artists.append(legend)

    ax.set_ylabel(unit)
    ax.set_yscale(yscale)
    if (yscale == "log"):
        ax.set_ymargin(0.3)
    else:
        ax.set_ylim(0, maxBarHeight * 1.2)

    plt.savefig(output, format="svg", bbox_inches='tight', bbox_extra_artists=artists)

def parseKeyValueList(arg):
    result = []
    entries = arg.split(',')
    for entry in entries:
        result.append(parseKeyValue(entry))
    return result

def parseKeyWithValueList(arg):
    keyValues = arg.split('=')
    keyParts = parseKey(keyValues[0])
    if (len(keyValues) == 1):
        return [keyParts]

    values = parseValueList(keyValues[1])
    return [keyParts, values]

def parseKeyValue(arg):
    keyValue = arg.split('=')
    keyParts = parseKey(keyValue[0])
    return [keyParts, keyValue[1]]

def parseKeyList(arg):
    return [parseKey(entry) for entry in arg.split(',')]

def parseValueList(arg):
    return arg.split(',')

def parseKey(arg):
    return arg.split('/')


if __name__ == "__main__":
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-i", "--input", help="input directory", default="../results")
    argParser.add_argument("-f", "--filter", help="result filter")
    argParser.add_argument("-g", "--groups", help="value source for groups")
    argParser.add_argument("-b", "--bars", help="value source for bars")
    argParser.add_argument("-s", "--stacks", help="value sources for stacks")
    argParser.add_argument("-u", "--unit", help="unit for the y-axis", default="s / op")
    argParser.add_argument("-o", "--output", help="output file path", default="./chart.svg")
    argParser.add_argument("-y", "--yscale", help="define the scale for the y-axis", default="linear")
    argParser.add_argument("-c", "--colors", help="list of color codes", default="#373478,#702670,#93A638,#AC913A")
    args = argParser.parse_args()

    filterSpec = parseKeyValue(args.filter)
    groupSpec = parseKeyWithValueList(args.groups)
    barSpec = parseKeyWithValueList(args.bars)
    stackSpecs = parseKeyValueList(args.stacks)
    colors = parseValueList(args.colors)

    results, groups, bars, stacks = extractResults(args.input, filterSpec, groupSpec, barSpec, stackSpecs)
    createBarChart(results, groups, bars, stacks, args.unit, args.output, args.yscale, colors)

