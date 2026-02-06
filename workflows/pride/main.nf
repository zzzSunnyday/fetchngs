/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { PRIDE_METADATA           } from '../../modules/local/pride_metadata'
include { PRIDE_FETCH_SDRF         } from '../../modules/local/pride_fetch_sdrf'
include { PRIDE_DOWNLOAD           } from '../../modules/local/pride_download'
include { PRIDE_TO_SAMPLESHEET     } from '../../modules/local/pride_to_samplesheet'
include { softwareVersionsToYAML   } from '../../subworkflows/nf-core/utils_nfcore_pipeline'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow PRIDE {

    take:
    ids // channel: [ ids ]

    main:
    ch_versions = channel.empty()

    //
    // MODULE: Fetch project metadata and file listings from PRIDE API
    //
    PRIDE_METADATA (
        ids,
        params.pride_file_types ?: ''
    )
    ch_versions = ch_versions.mix(PRIDE_METADATA.out.versions.first())

    //
    // MODULE: Fetch pre-annotated SDRF from bigbio repository (optional)
    //
    if (params.pride_use_existing_sdrf) {
        PRIDE_FETCH_SDRF (
            ids
        )
        ch_versions = ch_versions.mix(PRIDE_FETCH_SDRF.out.versions.first())
    }

    //
    // MODULE: Download files from PRIDE (unless skipped)
    //
    if (!params.pride_skip_file_download) {
        PRIDE_DOWNLOAD (
            PRIDE_METADATA.out.data.map { accession, json, files_tsv -> [accession, files_tsv] },
            params.pride_download_categories ?: 'RAW'
        )
        ch_versions = ch_versions.mix(PRIDE_DOWNLOAD.out.versions.first())
    }

    //
    // Join metadata with SDRF by accession, defaulting to NO_FILE placeholder
    //
    if (params.pride_use_existing_sdrf) {
        PRIDE_METADATA.out.data
            .join(PRIDE_FETCH_SDRF.out.sdrf, by: 0, remainder: true)
            .map { items ->
                def accession = items[0]
                def json      = items[1]
                def files_tsv = items[2]
                def sdrf      = items[3] ?: file('NO_FILE')
                [accession, json, files_tsv, sdrf]
            }
            .set { ch_for_samplesheet }
    } else {
        PRIDE_METADATA.out.data
            .map { accession, json, files_tsv ->
                [accession, json, files_tsv, file('NO_FILE')]
            }
            .set { ch_for_samplesheet }
    }

    //
    // Determine downloaded directory path for samplesheet generation
    //
    def downloaded_dir = params.pride_skip_file_download ? '' : "${params.outdir}/pride"

    //
    // MODULE: Generate pipeline-specific samplesheet
    //
    PRIDE_TO_SAMPLESHEET (
        ch_for_samplesheet,
        params.nf_core_pipeline ?: '',
        downloaded_dir
    )
    ch_versions = ch_versions.mix(PRIDE_TO_SAMPLESHEET.out.versions.first())

    //
    // Merge samplesheets across all accessions
    //
    PRIDE_TO_SAMPLESHEET
        .out
        .samplesheet
        .collectFile(name:'tmp_samplesheet.tsv', newLine: true, keepHeader: true, sort: { it.baseName })
        .map { it.text.tokenize('\n').join('\n') }
        .collectFile(name:'samplesheet.tsv', storeDir: "${params.outdir}/samplesheet")
        .set { ch_samplesheet }

    //
    // Collate and save software versions
    //
    softwareVersionsToYAML(ch_versions)
        .collectFile(storeDir: "${params.outdir}/pipeline_info", name: 'nf_core_fetchngs_software_mqc_versions.yml', sort: true, newLine: true)

    emit:
    samplesheet = ch_samplesheet
    versions    = ch_versions.unique()
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
