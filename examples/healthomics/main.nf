nextflow.enable.dsl = 2

process GENERATE {
    tag "shard-${shard_id}"
    label 'generation'
    label (params.generation_fallback_accelerator ? 'generation_fallback' : 'generation_single')
    cpus params.generation_cpus.toInteger()
    memory '32 GB'
    maxForks 2
    // GPU selection lives in conf/healthomics.config (single accelerator or omicsResourceFallbackOrder).
    container params.container_image
    publishDir "${params.outdir}/shards", mode: 'copy'

    input:
    val shard_id
    path input_cif, stageAs: 'input/*'
    path model_bundle, stageAs: 'model'
    path settings, stageAs: 'settings.json'
    path runner, stageAs: 'workflow.py'

    output:
    path "shard-${shard_id}", emit: results

    script:
    def request = groovy.json.JsonOutput.toJson([
        input_cif: input_cif.toString(),
        model_bundle: model_bundle.toString(),
        settings: settings.toString(),
        shard_id: shard_id,
        num_shards: params.num_shards.toInteger(),
        container_image: params.container_image
    ]).bytes.encodeBase64().toString()
    """
    python3 workflow.py generate '${request}'
    """

    stub:
    def request = groovy.json.JsonOutput.toJson([
        input_cif: input_cif.toString(),
        model_bundle: model_bundle.toString(),
        settings: settings.toString(),
        shard_id: shard_id,
        num_shards: params.num_shards.toInteger(),
        container_image: params.container_image
    ]).bytes.encodeBase64().toString()
    """
    python3 workflow.py plan '${request}'
    """
}

process SUMMARIZE {
    container params.container_image
    cpus 1
    memory '2 GB'
    publishDir params.outdir, mode: 'copy'

    input:
    path shards
    path runner, stageAs: 'workflow.py'

    output:
    path 'summary', emit: report

    script:
    """
    python3 workflow.py summarize . ${params.num_shards.toInteger()}
    """
}

workflow {
    if (!params.input_cif || !params.model_bundle || !params.settings || !params.container_image) {
        error 'Set --input_cif, --model_bundle, --settings and --container_image; see examples/healthomics/README.md.'
    }
    if (!params.num_shards.toString().isInteger() || params.num_shards.toInteger() < 1) {
        error '--num_shards must be a positive integer.'
    }
    if (!params.generation_cpus.toString().isInteger() || params.generation_cpus.toInteger() < 1) {
        error '--generation_cpus must be a positive integer.'
    }
    // HealthOmics accelerator types with at least 24 GB of GPU memory. nvidia-l40s is the type
    // qualified by RFD4-Proteina; nvidia-l4 and nvidia-tesla-a10g (24 GB each) need a separate
    // memory check for the longest input and batch_size. Some Regions offer only a subset.
    def accelerators = ['nvidia-l40s', 'nvidia-l4', 'nvidia-tesla-a10g']
    if (!(params.generation_accelerator in accelerators)) {
        error "--generation_accelerator must be one of ${accelerators.join(', ')}."
    }
    if (params.generation_fallback_accelerator) {
        if (!(params.generation_fallback_accelerator in accelerators)) {
            error "--generation_fallback_accelerator must be one of ${accelerators.join(', ')}."
        }
        if (params.generation_fallback_accelerator == params.generation_accelerator) {
            error '--generation_fallback_accelerator must differ from --generation_accelerator.'
        }
    }
    if (!params.generation_wait_minutes.toString().isInteger() || params.generation_wait_minutes.toInteger() < 1) {
        error '--generation_wait_minutes must be a positive integer.'
    }
    def input_cif = file(params.input_cif, checkIfExists: true)
    def model_bundle = file(params.model_bundle, checkIfExists: true)
    def settings = file(params.settings, checkIfExists: true)
    def runner = file("${projectDir}/bin/workflow.py", checkIfExists: true)
    def shard_ids = channel.fromList((0..<params.num_shards.toInteger()).toList())

    GENERATE(shard_ids, input_cif, model_bundle, settings, runner)
    SUMMARIZE(GENERATE.out.results.collect(), runner)
}
