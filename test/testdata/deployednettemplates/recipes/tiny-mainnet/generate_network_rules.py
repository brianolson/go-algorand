import json

RELAY_BANDWIDTH = 1000
SAME_REGION_RELAY_TO_RELAY_LATENCY = 10
CROSS_REGION_NODE_BANDWIDTH_FACTOR = 0.8

countries = json.load(open('./data/countries.json'))
countryBandwidths = json.load(open('./data/bandwidth.json'))
latencies = json.load(open('./data/latency.json'))

continentToGroup = {
    "North America": "us",
    "Europe": "eu",
    "Asia Pacific": "ap",
    "Africa": "af",
    "Australia": "au",
}

latencyMap = {}

for latency in latencies:
    lms = latencyMap.get(latency['source'])
    if lms is None:
        lms = dict()
        latencyMap[latency['source']] = lms
    lms[latency['target']] = latency['latency']

countryToContinent = {c['country']:c['continent'] for c in countries}

continentBandwidths = {}
for countryBandwidth in countryBandwidths:
    continent = countryToContinent[countryBandwidth[0]]
    if not continent:
        print(countryBandwidth)
        continue
    cbb = continentBandwidths.get(continent)
    if cbb is None:
        cbb = {'bandwidths':[]}
        continentBandwidths[continent] = cbb
    cbb['bandwidths'].append(countryBandwidth[1])

def average(data):
    return sum(data) / len(data)

writer = open('./network_performance_rules', 'wt')

for source, sourceGroup in continentToGroup.items():
    for target, targetGroup in continentToGroup.items():
        bandwidth = average(continentBandwidths[source]['bandwidths'])
        latency = latencyMap[source][target]
        if sourceGroup == targetGroup:
            relay_to_relay_latency = SAME_REGION_RELAY_TO_RELAY_LATENCY
            node_bandwidth_factor = 1.0
        else:
            relay_to_relay_latency = latency
            node_bandwidth_factor = CROSS_REGION_NODE_BANDWIDTH_FACTOR
        stbw = round(bandwidth*node_bandwidth_factor)
        latencyrounded = round(latency)
        relay_to_relay_latency = round(relay_to_relay_latency)
        writer.write(f'{sourceGroup}-n {targetGroup}-r {stbw} {latencyrounded}\n')
        writer.write(f'{sourceGroup}-r {targetGroup}-n {RELAY_BANDWIDTH} {latencyrounded}\n')
        writer.write(f'{sourceGroup}-r {targetGroup}-r {RELAY_BANDWIDTH} {relay_to_relay_latency}\n')

writer.close()
