from datetime import datetime, timedelta
from httpx import HTTPStatusError
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.tree_helpers import (
    filter_by_path,
)
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker


class NexusService:
    def __init__(
        self,
        nexus: NexusClientManager,
        nexus_database_client: NexusDatabaseClient,
        tracker: Tracker,
    ):
        self.nexus = nexus
        self.nexus_database = nexus_database_client
        self.tracker = tracker

    def afgør_skema_ejerskab(self, skema: dict) -> dict | None:
        skema = self.nexus.hent_fra_reference(skema)

        historik = self.nexus.skemaer.hent_skema_historik(skema=skema)
        leverandør_audit = sorted(
            historik, key=lambda entry: entry["date"], reverse=False
        )

        if len(leverandør_audit) > 0:
            return leverandør_audit[0]["professional"]

        return None

    def aktive_indsatser_på_forløb(self, referencer, forløbsnavn: str) -> bool:
        filtrerede_indsats_referencer = filter_by_path(
            referencer,
            path_pattern=f"/*/{forløbsnavn}/Indsatser/basketGrantReference",
            active_pathways_only=True,
        )

        indsatser = self.nexus.indsatser.filtrer_indsats_referencer(
            indsats_referencer=filtrerede_indsats_referencer, kun_aktive=True
        )

        return len(indsatser) > 0

    def hent_medarbejder(self, referencer, forløbsnavn: str, kompensationssag=False):
        medarbejder_reference = filter_by_path(
            referencer,
            path_pattern=f"/Børn og Unge Grundforløb/{forløbsnavn}/professionalReference",
            active_pathways_only=True,
        )

        try:
            if len(medarbejder_reference) > 0:
                medarbejder = self.nexus.hent_fra_reference(medarbejder_reference[0])
                return medarbejder

            elif not kompensationssag:
                medarbejder_reference = filter_by_path(
                    referencer,
                    path_pattern="/Børn og Unge Grundforløb/professionalReference",
                    active_pathways_only=True,
                )

                if len(medarbejder_reference) > 0:
                    medarbejder = self.nexus.hent_fra_reference(
                        medarbejder_reference[0]
                    )
                    return medarbejder
        except Exception:
            return None

        return None

    def passiver_kompensationssag(self, skema: dict, referencer, borger: dict) -> str:
        fejl_besked = ""

        forløbsreference = filter_by_path(
            referencer,
            path_pattern="/Børn og Unge Grundforløb/Sag: Støtte til børn og unge med funktionsnedsættelse",
            active_pathways_only=True,
        )

        if len(forløbsreference) == 0:
            # Forløb er afsluttet, luk derfor opgave.
            return ""

        if self.aktive_indsatser_på_forløb(
            referencer=referencer, forløbsnavn=forløbsreference[0]["name"]
        ):
            medarbejder = self.afgør_skema_ejerskab(skema=skema)

            if medarbejder is None:
                return "Kunne ikke afgøre medarbejder på kompensationssag.\n\n"

            self.nexus.opgaver.opret_opgave(
                objekt=skema,
                opgave_type="BL - Passivering ikke muligt pga. aktiv indsats",
                titel="Passivering ikke mulig - Aktiv indsats",
                ansvarlig_organisation=medarbejder["primaryOrganization"]["name"],
                ansvarlig_medarbejder=medarbejder,
                start_dato=datetime.now().date(),
                forfald_dato=datetime.now().date() + timedelta(days=7),
                beskrivelse="""Passivering af sag er ikke mulig, da en eller flere indsatser fortsat er aktive på sagen.\n\n                                        
                            "Indsatser skal derfor afsluttes og efterfølgende skal denne opgave afsluttes."\n\n                                        
                            "Tyra vil herefter lukke sagen.""",
            )
            return "Passivering ikke mulig pga. aktiv indsats.\n\n"

        medarbejder = self.hent_medarbejder(
            referencer=referencer,
            forløbsnavn=forløbsreference[0]["name"],
            kompensationssag=True,
        )

        if medarbejder is not None:
            self.nexus.organisationer.fjern_medarbejder_fra_forløb(
                medarbejder_reference=medarbejder
            )

        self.nexus.forløb.luk_forløb(forløb_reference=forløbsreference[0])
        relationer = self.nexus.organisationer.hent_organisationer_for_borger(
            borger=borger
        )

        for relation in relationer:
            if (
                relation["organization"]["name"]
                == "Ungerådgivningen Special - Kompensation"
                or relation["organization"]["name"] == "Ungerådgivningen 3"
            ):
                self.nexus.organisationer.fjern_borger_fra_organisation(
                    organisations_relation=relation
                )
        return fejl_besked

    def passiver_socialsager(self, skema: dict, referencer, borger) -> str:
        fejl_besked = ""

        forløbsreferencer = filter_by_path(
            referencer,
            path_pattern="/Børn og Unge Grundforløb/patientPathwayReference",
            active_pathways_only=True,
        )

        for forløbsreference in forløbsreferencer:
            if (
                forløbsreference["name"]
                == "Sag: Støtte til børn og unge med funktionsnedsættelse"
            ):
                continue

            if self.aktive_indsatser_på_forløb(
                referencer=referencer, forløbsnavn=forløbsreference["name"]
            ):
                medarbejder = self.afgør_skema_ejerskab(skema=skema)

                if medarbejder is None:
                    fejl_besked += f"Kunne ikke afgøre medarbejder på socialsag: {forløbsreference['name']}.\n\n"
                    continue

                try:
                    self.nexus.opgaver.opret_opgave(
                        objekt=skema,
                        opgave_type="BL - Passivering ikke muligt pga. aktiv indsats",
                        titel="Passivering ikke mulig - Aktiv indsats",
                        ansvarlig_organisation=medarbejder["primaryOrganization"][
                            "name"
                        ],
                        ansvarlig_medarbejder=medarbejder,
                        start_dato=datetime.now().date(),
                        forfald_dato=datetime.now().date() + timedelta(days=7),
                        beskrivelse=f"""Passivering af sag er ikke mulig, da en eller flere indsatser fortsat er aktive på sagen {forløbsreference["name"]}.\n\n
                                    "Indsatser skal derfor afsluttes og efterfølgende skal denne opgave afsluttes."\n\n
                                    "Tyra vil herefter lukke sagen.""",
                    )
                except Exception:
                    fejl_besked += "Medarbejder har ikke en ansvarlig organisation, og der kunne derfor ikke oprettes en opgave om aktiv indsats på sagen.\n\n"

                fejl_besked += "Passivering ikke mulig pga. aktiv indsats.\n\n"
                continue

            medarbejder = self.hent_medarbejder(
                referencer=referencer, forløbsnavn=forløbsreference["name"]
            )

            if medarbejder is not None:
                self.nexus.organisationer.fjern_medarbejder_fra_forløb(
                    medarbejder_reference=medarbejder
                )

            try:
                self.nexus.forløb.luk_forløb(forløb_reference=forløbsreference)
            except HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                else:
                    raise  # Any other HTTP error is real and should fail

            relationer = self.nexus.organisationer.hent_organisationer_for_borger(
                borger=borger
            )

            for relation in relationer:
                if relation["organization"]["name"] in [
                    "Ungerådgivningen Social 1 - Rådgivere Børn",
                    "Ungerådgivningen Social 2 - Rådgivere Børn",
                    "Ungerådgivningen Special - Rådgivere Børn",
                    "Ungerådgivningen Ungeindsats - Rådgivere Børn",
                    "Ungerådgivningen 1",
                    "Ungerådgivningen 2",
                    "Ungerådgivningen 3",
                    "Familierådgivningen",
                    "Ungerådgivningen Social 1 - Vagt & Visitation",
                ]:
                    self.nexus.organisationer.fjern_borger_fra_organisation(
                        organisations_relation=relation
                    )

        return fejl_besked
