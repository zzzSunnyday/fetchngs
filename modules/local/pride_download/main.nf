
process PRIDE_DOWNLOAD {
    tag "$accession"
    label 'process_medium'
    label 'error_retry'

    conda "conda-forge::python=3.9.5"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/python:3.9--1' :
        'biocontainers/python:3.9--1' }"

    input:
    tuple val(accession), path(files_tsv)
    val category

    output:
    tuple val(accession), path("${accession}/*"), emit: files
    path "versions.yml", emit: versions

    script:
    def category_arg = category ? "--category '${category}'" : ''
    """
    pride_download.py \\
        ${files_tsv} \\
        ${accession} \\
        ${category_arg} \\
        --checksum \\
        -l INFO

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | sed 's/Python //g')
    END_VERSIONS
    """
}
