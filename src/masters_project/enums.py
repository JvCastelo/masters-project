from enum import StrEnum


class SondaStationEnums(StrEnum):
    """SONDA network station identifiers: enum name (location) maps to station code used in URLs/APIs."""

    BRASILIA = "BRB"
    CACHOEIRA_PAULISTA = "CPA"
    CAICO = "CAI"
    CAMPO_GRANDE_FAZENDA = "CGR"
    CAMPO_GRANDE_UNIDERP = "CGU"
    CAMPO_MOURAO = "CMS"
    CUIABA = "CBA"
    CURITIBA_TECPAR = "CTB"
    CURITIBA_UTFPR = "CTS"
    FLORIANOPOLIS_BSRN = "FLN"
    FLORIANOPOLIS_SAPIENS = "SPK"
    JOINVILLE = "JOI"
    MEDIANEIRA = "MDS"
    NATAL = "NAT"
    OURINHOS = "ORN"
    PALMAS = "PMA"
    PETROLINA = "PTR"
    SANTAREM = "STM"
    SAO_LUIZ = "SLZ"
    SAO_MARTINHO = "SMS"
    SOMBRIO = "SBR"


class GoesChannelEnums(StrEnum):
    """GOES-R ABI channel identifiers (C01–C16) for band selection in file discovery and processing."""

    C01 = "C01"
    C02 = "C02"
    C03 = "C03"
    C04 = "C04"
    C05 = "C05"
    C06 = "C06"
    C07 = "C07"
    C08 = "C08"
    C09 = "C09"
    C10 = "C10"
    C11 = "C11"
    C12 = "C12"
    C13 = "C13"
    C14 = "C14"
    C15 = "C15"
    C16 = "C16"
