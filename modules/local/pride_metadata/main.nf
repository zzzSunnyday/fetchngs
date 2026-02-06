
process PRIDE_METADATA {
    tag "$id"
    label 'process_single'
    label 'error_retry'

    conda "conda-forge::python=3.9.5"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/python:3.9--1' :
        'biocontainers/python:3.9--1' }"

    input:
    val id
    val file_types

    output:
    tuple val(id), path("*.metadata.json"), path("*.files.tsv"), emit: data
    path "*.metadata.tsv" , emit: tsv
    path "versions.yml"   , emit: versions

    script:
    def file_types_arg = file_types ? "--file-types '${file_types}'" : ''
    """
    pride_metadata.py \\
        ${id} \\
        ${id}.metadata.tsv \\
        ${id}.metadata.json \\
        ${id}.files.tsv \\
        ${file_types_arg}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | sed 's/Python //g')
    END_VERSIONS
    """
}
