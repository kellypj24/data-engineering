from dataclasses import dataclass


@dataclass
class FileGenerationReportHeader:
    name: str
    mapping: dict


class FileGenerationReportHeaders:
    BCBSM_SUPPLEMENTAL = FileGenerationReportHeader(
        name="bcbsm_supplemental",
        mapping={
            "memberkey": "MemberKey",
            "providerkey": "ProviderKey",
            "dos": "DOS",
            "dosthru": "DOSThru",
            "providertype": "ProviderType",
            "providertaxonomy": "ProviderTaxonomy",
            "cptpx": "CPTPx",
            "hcpcpspx": "HCPCPSPx",
            "loinc": "LOINC",
            "snowmed": "SNOWMED",
            "icddx": "ICDDX",
            "icddx10": "ICDDX10",
            "rxnorm": "RxNorm",
            "cvx": "CVX",
            "modifier": "Modifier",
            "rxproviderflag": "RxProviderFlag",
            "pcpflag": "PCPFlag",
            "quantitydispensed": "QuantityDispensed",
            "icdpx": "ICDPx",
            "icdpx10": "ICDPx10",
            "suppsource": "SuppSource",
            "result": "Result",
        },
    )
