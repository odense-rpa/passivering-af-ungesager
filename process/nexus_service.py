from datetime import datetime, timedelta
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

    def hent_medarbejder(self, referencer, forløbsnavn: str):
        medarbejder_reference = filter_by_path(
            referencer,
            path_pattern=f"/Børn og Unge Grundforløb/{forløbsnavn}/professionalReference",
            active_pathways_only=True,
        )

        if len(medarbejder_reference) > 0:
            medarbejder = self.nexus.hent_fra_reference(medarbejder_reference[0])
            return medarbejder

        else:
            medarbejder_reference = filter_by_path(
                referencer,
                path_pattern="/Børn og Unge Grundforløb/professionalReference",
                active_pathways_only=True,
            )

            if len(medarbejder_reference) > 0:
                medarbejder = self.nexus.hent_fra_reference(medarbejder_reference[0])
                return medarbejder

        return None

    def passiver_kompensationssag(self, skema: dict, referencer, borger) -> str:
        fejl_besked = ""

        forløbsreference = filter_by_path(
            referencer,
            path_pattern="/Børn og Unge Grundforløb/Sag: Støtte til børn og unge med funktionsnedsættelse",
            active_pathways_only=True,
        )

        if len(forløbsreference) == 0:
            fejl_besked += "Kunne ikke finde aktivkompensationssag."
            return fejl_besked

        medarbejder = self.hent_medarbejder(
            referencer=referencer, forløbsnavn=forløbsreference[0]["name"]
        )

        if self.aktive_indsatser_på_forløb(
            referencer=referencer, forløbsnavn=forløbsreference[0]["name"]
        ):
            if medarbejder is not None:
                medarbejder = self.nexus_database.hent_medarbejder_med_activity_id(
                    medarbejder.get("activityIdentifier", {}).get("activityId", "")
                )
                medarbejder = self.nexus.organisationer.hent_medarbejder_ved_initialer(
                    medarbejder[0].get("primary_identifier", "")
                )

            if medarbejder is None:
                fejl_besked = "Kunne ikke finde medarbejder på kompensationssag."
                return fejl_besked

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
            fejl_besked = "Passivering ikke mulig pga. aktiv indsats."

        if medarbejder is not None:
            self.nexus.organisationer.fjern_medarbejder_fra_forløb(
                medarbejder_reference=medarbejder
            )

        self.nexus.forløb.luk_forløb(forløb_reference=forløbsreference[0])
        relationer = self.nexus.organisationer.hent_organisationer_for_borger(
            borger=borger
        )

        for relation in relationer:
            if relation["name"] == "Ungerådgivningen Special - Kompensation":
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

            medarbejder = self.hent_medarbejder(
                referencer=referencer, forløbsnavn=forløbsreference["name"]
            )

            if self.aktive_indsatser_på_forløb(
                referencer=referencer, forløbsnavn=forløbsreference["name"]
            ):
                if medarbejder is not None:
                    medarbejder = self.nexus_database.hent_medarbejder_med_activity_id(
                        medarbejder.get("activityIdentifier", {}).get("activityId", "")
                    )
                    medarbejder = (
                        self.nexus.organisationer.hent_medarbejder_ved_initialer(
                            medarbejder[0].get("primary_identifier", "")
                        )
                    )

                if medarbejder is None:
                    fejl_besked = "Kunne ikke finde medarbejder på kompensationssag."
                    return fejl_besked

                self.nexus.opgaver.opret_opgave(
                    objekt=skema,
                    opgave_type="BL - Passivering ikke muligt pga. aktiv indsats",
                    titel="Passivering ikke mulig - Aktiv indsats",
                    ansvarlig_organisation=medarbejder["primaryOrganization"]["name"],
                    ansvarlig_medarbejder=medarbejder,
                    start_dato=datetime.now().date(),
                    forfald_dato=datetime.now().date() + timedelta(days=7),
                    beskrivelse=f"""Passivering af sag er ikke mulig, da en eller flere indsatser fortsat er aktive på sagen {forløbsreference["name"]}.\n\n                                        
                                "Indsatser skal derfor afsluttes og efterfølgende skal denne opgave afsluttes."\n\n                                        
                                "Tyra vil herefter lukke sagen.""",
                )
                fejl_besked += "Passivering ikke mulig pga. aktiv indsats."
                return fejl_besked

            if medarbejder is not None:
                self.nexus.organisationer.fjern_medarbejder_fra_forløb(
                    medarbejder_reference=medarbejder
                )

            self.nexus.forløb.luk_forløb(forløb_reference=forløbsreference)
            relationer = self.nexus.organisationer.hent_organisationer_for_borger(
                borger=borger
            )

            for relation in relationer:
                if relation["organization"]["name"] in [
                    "Ungerådgivningen Social 1 - Rådgivere Børn",
                    "Ungerådgivningen Social 2 - Rådgivere Børn",
                    "Ungerådgivningen Special - Rådgivere Børn",
                    "Ungerådgivningen Ungeindsats - Rådgivere Børn",
                ]:
                    self.nexus.organisationer.fjern_borger_fra_organisation(
                        organisations_relation=relation
                    )

        return fejl_besked
