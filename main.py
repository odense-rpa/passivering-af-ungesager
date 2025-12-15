import asyncio
import logging
import sys

from automation_server_client import (
    AutomationServer,
    Workqueue,
    WorkItemError,
    Credential,
    WorkItemStatus,
)
from datetime import datetime, timedelta, timezone
from kmd_nexus_client import NexusClientManager
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from odk_tools.reporting import report
from process.nexus_service import NexusService

nexus: NexusClientManager
nexus_database_client: NexusDatabaseClient
nexus_service: NexusService
tracker: Tracker

proces_navn = "Passivering af ungesager"


async def populate_queue(workqueue: Workqueue):
    aktivitetsliste = nexus.aktivitetslister.hent_aktivitetsliste(
        navn="Opgaver til Tyra", organisation=None, medarbejder=None, antal_sider=10
    )

    if not aktivitetsliste:
        raise ValueError(
            "Ingen aktiviteter fundet i aktivitetslisten 'Opgaver til Tyra'"
        )

    aktivitetsliste = [
        aktivitet
        for aktivitet in aktivitetsliste
        if aktivitet["status"] == "Aktiv"
        and aktivitet["name"] == "Luk sag - Tyra"
        and datetime.strptime(aktivitet["date"], "%Y-%m-%dT%H:%M:%S.%f%z")
        > datetime.now(timezone.utc) - timedelta(days=7)
    ]

    for aktivitet in aktivitetsliste:
        eksisterende_kødata = workqueue.get_item_by_reference(
            str(aktivitet["id"]), status=WorkItemStatus.COMPLETED
        )
        eksisterende_kødata = [
            item
            for item in eksisterende_kødata
            if item.updated_at > datetime.now() - timedelta(days=7)
        ]

        if len(eksisterende_kødata) == 0:
            workqueue.add_item(aktivitet, reference=str(aktivitet["id"]))


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    for item in workqueue:
        with item:
            data = item.data

            try:
                borger = nexus.hent_fra_reference(data["patients"][0])
                skema = nexus.hent_fra_reference(data["children"][0])
                opgave = nexus.hent_fra_reference(data)

                if skema["pathwayAssociation"]["placement"] is None:
                    fejl_besked = "Skema er ikke tilknyttet et forløb."
                
                else:                    
                    kompensationssag = (
                        skema["pathwayAssociation"]["placement"]["name"]
                        == "Sag: Støtte til børn og unge med funktionsnedsættelse"
                    )
                    pathway = nexus.borgere.hent_visning(borger=borger)

                    if pathway is None:
                        raise ValueError(
                            f"Kunne ikke finde -Alt for borger {borger['patientIdentifier']['identifier']}"
                        )

                    referencer = nexus.borgere.hent_referencer(visning=pathway)
                    fejl_besked = ""

                    if kompensationssag:
                        fejl_besked += nexus_service.passiver_kompensationssag(
                            skema=skema, referencer=referencer, borger=borger
                        )
                    else:
                        fejl_besked += nexus_service.passiver_socialsager(
                            skema=skema, referencer=referencer, borger=borger
                        )

                if fejl_besked:
                    report(
                        report_id="passivering_af_ungesager",
                        group="Manuel behandling",
                        json={
                            "Cpr": borger["patientIdentifier"]["identifier"],
                            "Fejlmeddelelse": fejl_besked,
                        },
                    )

                    # Udskyd opgave deadline med 1 uge
                    date_obj = datetime.strptime(opgave["dueDate"], "%Y-%m-%d")
                    new_date = date_obj + timedelta(weeks=1)
                    opgave["dueDate"] = new_date.strftime("%Y-%m-%d")
                    nexus.opgaver.rediger_opgave(opdateret_opgave=opgave)
                    tracker.track_partial_task(process_name=proces_navn)
                else:
                    nexus.opgaver.luk_opgave(opgave=opgave)
                    report(
                        report_id="passivering_af_ungesager",
                        group="Behandlet",
                        json={"Cpr": borger["patientIdentifier"]["identifier"]},
                    )
                    tracker.track_task(process_name=proces_navn)

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")
    tracking_credential = Credential.get_credential("Odense SQL Server")

    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )

    nexus_database_client = NexusDatabaseClient(
        host=nexus_database_credential.data["hostname"],
        port=nexus_database_credential.data["port"],
        user=nexus_database_credential.username,
        password=nexus_database_credential.password,
        database=nexus_database_credential.data["database_name"],
    )

    nexus_service = NexusService(
        nexus=nexus, nexus_database_client=nexus_database_client, tracker=tracker
    )

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
