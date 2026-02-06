
process PRIDE_TO_SAMPLESHEET {
    tag "$accession"
    label 'process_single'

    conda "conda-forge::python=3.9.5"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/python:3.9--1' :
        'biocontainers/python:3.9--1' }"

    input:
    tuple val(accession), path(metadata_json), path(files_tsv), path(sdrf)
    val pipeline
    val downloaded_dir

    output:
    path "*.samplesheet.tsv", emit: samplesheet
    path "versions.yml"     , emit: versions

    script:
    def pipeline_arg = pipeline ? "--pipeline '${pipeline}'" : ''
    def sdrf_arg = sdrf.name != 'NO_FILE' ? "--sdrf ${sdrf}" : ''
    def downloaded_dir_arg = downloaded_dir ? "--downloaded-dir '${downloaded_dir}'" : ''
    """
    pride_to_samplesheet.py \\
        ${accession} \\
        ${metadata_json} \\
        ${files_tsv} \\
        . \\
        ${pipeline_arg} \\
        ${sdrf_arg} \\
        ${downloaded_dir_arg}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | sed 's/Python //g')
    END_VERSIONS
    """
}
