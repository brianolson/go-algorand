all:	network_performance_rules

network_performance_rules:	generate_network_rules.py data/bandwidth.json data/countries.json data/latency.json configs/node.json configs/nonPartNode.json configs/relay.json network-tpl.json
	python3 generate_network_rules.py
	../../generate-recipe/generate_network.py --template ./network-tpl.json

clean:
	rm -f network_performance_rules
