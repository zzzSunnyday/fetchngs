
process PRIDE_FETCH_SDRF {
    tag "$accession"
    label 'process_single'
    label 'error_retry'

    conda "conda-forge::wget=1.21.4"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/wget:1.21.4' :
        'biocontainers/wget:1.21.4' }"

    input:
    val accession

    output:
    tuple val(accession), path("*.sdrf.tsv"), optional: true, emit: sdrf
    path "versions.yml"                     , emit: versions

    script:
    def sdrf_url = "https://raw.githubusercontent.com/bigbio/proteomics-sample-metadata/master/annotated-projects/${accession}/${accession}.sdrf.tsv"
    """
    wget -q -O ${accession}.sdrf.tsv "${sdrf_url}" || rm -f ${accession}.sdrf.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        wget: \$(echo \$(wget --version | head -n 1 | sed 's/^GNU Wget //; s/ .*\$//'))
    END_VERSIONS
    """
}
