# orch-service-yorc-dynamic-orchestration-plugin

<a href="https://doi.org/10.5281/zenodo.6080486"><img src="https://zenodo.org/badge/DOI/10.5281/zenodo.6080486.svg" alt="DOI"></a>

The Yorc Dynamic Orchestration plugin implements a Yorc ([Ystia orchestrator](https://github.com/ystia/yorc/)) plugin as described in [Yorc documentation](https://yorc.readthedocs.io/en/latest/plugins.html), allowing the orchestrator
to dynamically change the location of infrastructure resources to create in a workflow.

## Acknowledgement
This code repository is a result / contains results of the LEXIS project. The project has received funding from the European Union’s Horizon 2020 Research and Innovation programme (2014-2020) under grant agreement No. 825532.

## TOSCA components

This plugin provides the following TOSCA components defined in the TOSCA file [a4c/dynamic-orchestration-types-a4c.yaml](a4c/dynamic-orchestration-types-a4c.yaml)
that can be uploaded in [Alien4Cloud](https://alien4cloud.github.io/) catalog of TOSCA components:

### org.lexis.common.dynamic.orchestration.nodes.SetLocationsJob
Compute the best locations where to allocate the associated infrastructure resources components

### org.lexis.common.dynamic.orchestration.nodes.ValidateAndExchangeToken
Validate an input token and exchange it for the orchestrator client

### org.lexis.common.dynamic.orchestration.nodes.RefreshTargetTokens
Refreshes the access token attribute of the associated target

## To build this plugin

You need first to have a working [Go environment](https://golang.org/doc/install).
Then to build, execute the following instructions:

```
mkdir -p $GOPATH/src/github.com/lexis-project
cd $GOPATH/src/github.com/lexis-project
git clone https://github.com/lexis-project/yorc-dynamic-orchestration-plugin
cd yorc-dynamic-orchestration-plugin
make
```

The plugin is then available at `bin/dyn-orchestration-plugin`.

## Licensing

This plugin is licensed under the [Apache 2.0 License](LICENSE).
