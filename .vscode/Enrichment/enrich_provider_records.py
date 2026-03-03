"""
enrich_provider_records.py
──────────────────────────
Microservice class that enriches Provider records with county information
sourced from the CountyZipEnriched_GDP collection.

Dependencies:
    Python  : 3.11.9
    pymongo : 4.16.0
    Target  : MongoDB Atlas (current)

Bug fixed (v2)
──────────────
The original ProcessAllRecords iterated over ZIP codes sourced from
CountyZipEnriched_GDP.  Any Provider whose effective ZIP was absent from
that reference table was silently skipped, yielding only ~1 % of the
expected output.

The corrected approach:
  1. Pre-loads the entire CountyZipEnriched_GDP collection into an
     in-memory dict (zip → county_info | None) once at construction time.
     This replaces per-ZIP round-trips inside worker threads.
  2. Streams ALL Provider documents using cursor-based (keyset) pagination
     so that every record is enriched regardless of whether its ZIP exists
     in the reference table (CountyInfo is set to null when there is no
     match or when the match is below the 99 % threshold).
"""

from __future__ import annotations

import copy
import logging
import queue
import threading
import time
from decimal import Decimal
from typing import Any

import pymongo
from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, PyMongoError

# ──────────────────────────────────────────────────────────────────────────────
# Module-level logger
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
COUNTY_INFO_FIELDS: tuple[str, ...] = (
    "countyFips",
    "countyName",
    "mainCity",
    "percentOfZipInCounty",
    "realGDP2023",
    "gdpRankInState2023",
    "percentChange2023",
    "percentChangeRankInState2023",
)

# Business rule: a zip is wholly in one county when percentOfZipInCounty >= 99 %
COUNTY_MATCH_THRESHOLD = Decimal("0.99")

# Retry / batching knobs
MAX_RETRIES: int = 4
RETRY_BASE_DELAY_SECONDS: float = 1.0    # exponential back-off base (seconds)
BATCH_SIZE: int = 1_000                  # Provider docs per bulk_write call
PAGE_SIZE: int = 5_000                   # Provider docs fetched per DB page
MAX_WORKERS: int = 8                     # parallel enrichment/write threads

# Sentinel placed on the work queue to signal workers to stop
_QUEUE_DONE = object()


# ──────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ──────────────────────────────────────────────────────────────────────────────

def _to_decimal(value: Any) -> Decimal:
    """
    Safely convert a value that may be a pymongo Decimal128, a plain Decimal,
    a float, or a string into a Python Decimal for comparison.
    """
    if hasattr(value, "to_decimal"):   # pymongo Decimal128
        return value.to_decimal()
    return Decimal(str(value))


def _extract_zip(provider: dict) -> str | None:
    """
    Apply the Provider zip-selection business rule:

    •  Prefer the Practice Location postal code when it is non-null/non-empty.
    •  Fall back to the Mailing Address postal code otherwise.
    •  Normalise to the first five characters to handle ZIP+4 suffixes.
    """
    practice_zip: str | None = provider.get(
        "Provider Business Practice Location Address Postal Code"
    )
    mailing_zip: str | None = provider.get(
        "Provider Business Mailing Address Postal Code"
    )

    raw_zip = practice_zip if practice_zip else mailing_zip
    if not raw_zip:
        return None
    return str(raw_zip).strip()[:5]


def _build_county_info(county_rec: dict) -> dict:
    """Extract the enrichment fields from one CountyZipEnriched_GDP document."""
    return {field: county_rec.get(field) for field in COUNTY_INFO_FIELDS}


# ──────────────────────────────────────────────────────────────────────────────
# Main class
# ──────────────────────────────────────────────────────────────────────────────

class EnrichProviderRecordsWithCountyData:
    """
    Reads Provider records and CountyZipEnriched_GDP records from an Atlas
    database and writes enriched copies into the EnrichedProvider collection.

    Parameters
    ----------
    argMongoDataBaseClient : pymongo.MongoClient
        An already-connected MongoClient instance.
    argDatabaseName : str
        Name of the Atlas database.  Default: 'PublicHealthData'
    argProviderCollectionName : str
        Default: 'Provider'
    argZipCountyEnrichmentCollectionName : str
        Default: 'CountyZipEnriched_GDP'
    argEnrichedProviderCollectionName : str
        Default: 'EnrichedProvider'
    """

    # ------------------------------------------------------------------ #
    #  Constructor                                                         #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        argMongoDataBaseClient: MongoClient,
        argDatabaseName: str = "PublicHealthData",
        argProviderCollectionName: str = "Provider",
        argZipCountyEnrichmentCollectionName: str = "CountyZipEnriched_GDP",
        argEnrichedProviderCollectionName: str = "EnrichedProvider",
    ) -> None:

        # ── 1a. Store constructor arguments as instance variables ──────────
        self.argMongoDataBaseClient: MongoClient = argMongoDataBaseClient
        self.argDatabaseName: str = argDatabaseName
        self.argProviderCollectionName: str = argProviderCollectionName
        self.argZipCountyEnrichmentCollectionName: str = argZipCountyEnrichmentCollectionName
        self.argEnrichedProviderCollectionName: str = argEnrichedProviderCollectionName

        # ── Resolve database ───────────────────────────────────────────────
        _db = argMongoDataBaseClient[argDatabaseName]

        # ── 1a. Create Collection instance variables ───────────────────────
        self.ProviderCollection: Collection = _db[argProviderCollectionName]
        self.ZipCountyEnrichmentCollection: Collection = (
            _db[argZipCountyEnrichmentCollectionName]
        )
        self.EnrichedProviderCollection: Collection = (
            _db[argEnrichedProviderCollectionName]
        )

        # ── 1a-i. Empty the enriched collection if it already exists ───────
        existing_collections = _db.list_collection_names()
        if argEnrichedProviderCollectionName in existing_collections:
            deleted = self.EnrichedProviderCollection.delete_many({})
            logger.info(
                "Cleared existing '%s' collection – %d document(s) removed.",
                argEnrichedProviderCollectionName,
                deleted.deleted_count,
            )

        # ── 2. Guard against empty source collections ──────────────────────
        try:
            if self.ProviderCollection.estimated_document_count() == 0:
                raise ValueError(
                    f"Collection '{argProviderCollectionName}' is empty. "
                    "Enrichment cannot proceed without Provider records."
                )
            if self.ZipCountyEnrichmentCollection.estimated_document_count() == 0:
                raise ValueError(
                    f"Collection '{argZipCountyEnrichmentCollectionName}' is empty. "
                    "Enrichment cannot proceed without ZIP/county reference data."
                )
        except PyMongoError as exc:
            raise RuntimeError(
                f"Unexpected error while verifying source collections: {exc}"
            ) from exc

        # ── 3. Build ascending sorted tuple of unique ZIP codes ────────────
        #       Source: CountyZipEnriched_GDP (the reference / enrichment side).
        #       Used by GetProvidersByZip / GetCountyZipEnriched_GDPRecords.
        try:
            raw_zips: list[str] = self.ZipCountyEnrichmentCollection.distinct("zip")
        except PyMongoError as exc:
            raise RuntimeError(
                f"Failed to retrieve distinct ZIP codes from "
                f"'{argZipCountyEnrichmentCollectionName}': {exc}"
            ) from exc

        if not raw_zips:
            raise ValueError(
                f"No ZIP codes found in '{argZipCountyEnrichmentCollectionName}'."
            )

        self._zip_codes: tuple[str, ...] = tuple(sorted(raw_zips))

        # ── 4. NextZipCode cursor (thread-safe) ────────────────────────────
        self._zip_index: int = 0
        self._zip_lock: threading.Lock = threading.Lock()
        self.NextZipCode: str = self._zip_codes[0]

        # ── 5. Pre-load enrichment lookup into memory ──────────────────────
        #       Key  : 5-digit zip string
        #       Value: county_info dict when percentOfZipInCounty >= 99 %,
        #              otherwise None.
        #
        #       Building this once eliminates all per-ZIP round-trips inside
        #       ProcessAllRecords worker threads, which is critical when
        #       iterating over 700 M+ Provider records.
        #
        #       Memory estimate: ~42 k US ZIP codes × ~10 small fields
        #       ≈ well within normal process limits.
        logger.info("Pre-loading county enrichment lookup into memory …")
        self._county_lookup: dict[str, dict | None] = {}
        try:
            projection = {"_id": 0, "zip": 1, **{f: 1 for f in COUNTY_INFO_FIELDS}}
            cursor = self.ZipCountyEnrichmentCollection.find({}, projection)
            for rec in cursor:
                zip_key: str = str(rec.get("zip", "")).strip()[:5]
                if not zip_key:
                    continue
                pct_raw = rec.get("percentOfZipInCounty")
                qualifies = (
                    pct_raw is not None
                    and _to_decimal(pct_raw) >= COUNTY_MATCH_THRESHOLD
                )
                if qualifies and zip_key not in self._county_lookup:
                    # Unambiguous single-county ZIP – store enrichment data.
                    self._county_lookup[zip_key] = _build_county_info(rec)
                elif zip_key not in self._county_lookup:
                    # Below threshold or missing percentage – mark unknown.
                    self._county_lookup[zip_key] = None
        except PyMongoError as exc:
            raise RuntimeError(
                f"Failed to pre-load county enrichment data: {exc}"
            ) from exc

        logger.info(
            "County enrichment lookup ready – %d ZIP codes loaded "
            "(%d unambiguous, %d ambiguous/unknown).",
            len(self._county_lookup),
            sum(1 for v in self._county_lookup.values() if v is not None),
            sum(1 for v in self._county_lookup.values() if v is None),
        )

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def GetProvidersByZip(self, ArgZip: str) -> list[dict]:
        """
        Returns deep copies of all Provider records whose effective ZIP code
        (practice location preferred, mailing address as fallback) matches
        *ArgZip*.

        As a side-effect the internal NextZipCode pointer is advanced to the
        next ZIP in the sorted sequence.  This method is thread-safe.

        Parameters
        ----------
        ArgZip : str
            The ZIP code to query.  Callers should read ``self.NextZipCode``
            and call ``GetCountyZipEnriched_GDPRecords`` with that value
            *before* calling this function, since this call advances the
            internal cursor.

        Returns
        -------
        list[dict]
            Deep copies of matching Provider documents.
        """
        with self._zip_lock:
            self._zip_index += 1
            if self._zip_index < len(self._zip_codes):
                self.NextZipCode = self._zip_codes[self._zip_index]
            else:
                self.NextZipCode = ""  # sentinel: exhausted

        # Business rule query: prefer practice-location ZIP; fall back to
        # mailing ZIP only when practice-location ZIP is null/missing/empty.
        query = {
            "$or": [
                {
                    "Provider Business Practice Location Address Postal Code": {
                        "$regex": f"^{ArgZip}"
                    }
                },
                {
                    "$and": [
                        {
                            "$or": [
                                {
                                    "Provider Business Practice Location Address Postal Code": None
                                },
                                {
                                    "Provider Business Practice Location Address Postal Code": {
                                        "$exists": False
                                    }
                                },
                                {
                                    "Provider Business Practice Location Address Postal Code": ""
                                },
                            ]
                        },
                        {
                            "Provider Business Mailing Address Postal Code": {
                                "$regex": f"^{ArgZip}"
                            }
                        },
                    ]
                },
            ]
        }

        try:
            records = list(self.ProviderCollection.find(query))
        except PyMongoError as exc:
            raise RuntimeError(
                f"Database error while fetching providers for ZIP '{ArgZip}': {exc}"
            ) from exc

        return [copy.deepcopy(r) for r in records]

    # ------------------------------------------------------------------ #

    def GetCountyZipEnriched_GDPRecords(self, argZipCode: str) -> list[dict]:
        """
        Returns all CountyZipEnriched_GDP documents that match *argZipCode*.

        Callers should invoke this method with the current value of
        ``self.NextZipCode`` *before* calling ``GetProvidersByZip`` so that
        both functions operate on the same ZIP before the cursor advances.

        Parameters
        ----------
        argZipCode : str
            The ZIP code to look up.

        Returns
        -------
        list[dict]
            Matching CountyZipEnriched_GDP documents.
        """
        try:
            return list(self.ZipCountyEnrichmentCollection.find({"zip": argZipCode}))
        except PyMongoError as exc:
            raise RuntimeError(
                f"Database error while fetching county/ZIP data for "
                f"ZIP '{argZipCode}': {exc}"
            ) from exc

    # ------------------------------------------------------------------ #

    def ProcessProviderRecord(
        self,
        ArgProviderRecord: dict,
        ArgCountyZipEnriched_GDPRecords: list[dict],
    ) -> bool:
        """
        Builds and inserts a single EnrichedProvider document.

        Parameters
        ----------
        ArgProviderRecord : dict
            A Provider document (as returned by ``GetProvidersByZip``).
        ArgCountyZipEnriched_GDPRecords : list[dict]
            All CountyZipEnriched_GDP records that share the provider's ZIP.

        Returns
        -------
        bool
            ``True`` on success.

        Raises
        ------
        RuntimeError
            If the enriched record already exists or a database error occurs.
        """
        npi = ArgProviderRecord.get("NPI", "<unknown NPI>")

        existing = self.EnrichedProviderCollection.find_one(
            {"NPI": npi}, projection={"_id": 1}
        )
        if existing:
            raise RuntimeError(
                f"EnrichedProvider record for NPI '{npi}' already exists "
                f"(document _id: {existing['_id']}). Duplicate insertion aborted."
            )

        county_info: dict | None = None
        for county_rec in ArgCountyZipEnriched_GDPRecords:
            pct_raw = county_rec.get("percentOfZipInCounty")
            if pct_raw is None:
                continue
            if _to_decimal(pct_raw) >= COUNTY_MATCH_THRESHOLD:
                county_info = _build_county_info(county_rec)
                break

        enriched: dict = copy.deepcopy(ArgProviderRecord)
        enriched.pop("_id", None)
        enriched["CountyInfo"] = county_info

        try:
            self.EnrichedProviderCollection.insert_one(enriched)
        except pymongo.errors.DuplicateKeyError as exc:
            raise RuntimeError(
                f"Duplicate key error inserting EnrichedProvider for NPI '{npi}': {exc}"
            ) from exc
        except PyMongoError as exc:
            raise RuntimeError(
                f"Database error inserting EnrichedProvider for NPI '{npi}': {exc}"
            ) from exc

        return True

    # ------------------------------------------------------------------ #

    def ProcessAllRecords(self) -> bool:
        """
        Iterates over **every** Provider record, enriches each one with county
        data, and bulk-writes the results into the EnrichedProvider collection.

        Design
        ~~~~~~
        •  **Completeness** – iteration is driven by the Provider collection
           (not CountyZipEnriched_GDP), so every provider is processed.
           Providers whose ZIP has no enrichment match receive CountyInfo: null.

        •  **In-memory lookup** – county data was pre-loaded into
           ``self._county_lookup`` at construction time (zip → county_info).
           Workers never query CountyZipEnriched_GDP during this phase.

        •  **Keyset (cursor-based) pagination** – uses ``_id > last_id``
           rather than ``skip`` to page through 700 M+ documents efficiently.
           skip() costs O(offset) in MongoDB; keyset pagination is O(1).

        •  **Producer / consumer** – a single producer thread pages through
           Provider and feeds pages onto a bounded queue.  ``MAX_WORKERS``
           consumer threads pull pages, enrich them, and bulk-write.
           The bounded queue (maxsize = MAX_WORKERS * 2) prevents the producer
           from running arbitrarily far ahead and exhausting memory.

        •  **Retry with exponential back-off** – each ``bulk_write`` is
           retried up to ``MAX_RETRIES`` times with doubling delays before
           the call is abandoned.

        Returns
        -------
        bool
            ``True`` when all records have been processed successfully.

        Raises
        ------
        RuntimeError
            States how many records were successfully inserted and why
            processing failed.
        """
        total_inserted_lock = threading.Lock()
        total_inserted: int = 0
        failure_info: list[str] = []   # written by workers; read after join

        # Bounded queue: limits how far the producer runs ahead of consumers,
        # keeping memory usage predictable for 700 M+ record sets.
        work_queue: queue.Queue[Any] = queue.Queue(maxsize=MAX_WORKERS * 2)

        # ── Retry-aware bulk writer ────────────────────────────────────────
        def _flush_batch(batch: list[dict]) -> int:
            ops = [pymongo.InsertOne(doc) for doc in batch]
            last_exc: Exception | None = None

            for attempt in range(MAX_RETRIES + 1):
                try:
                    result = self.EnrichedProviderCollection.bulk_write(
                        ops, ordered=False
                    )
                    return result.inserted_count
                except BulkWriteError as bwe:
                    inserted_so_far: int = bwe.details.get("nInserted", 0)
                    logger.error(
                        "BulkWriteError attempt %d/%d – %d inserted, %d errors.",
                        attempt + 1, MAX_RETRIES + 1,
                        inserted_so_far,
                        len(bwe.details.get("writeErrors", [])),
                    )
                    # Partial writes are already committed – do not retry.
                    raise
                except PyMongoError as exc:
                    last_exc = exc
                    if attempt < MAX_RETRIES:
                        delay = RETRY_BASE_DELAY_SECONDS * (2 ** attempt)
                        logger.warning(
                            "Transient error bulk_write attempt %d/%d "
                            "(retry in %.1f s): %s",
                            attempt + 1, MAX_RETRIES + 1, delay, exc,
                        )
                        time.sleep(delay)
                    else:
                        raise RuntimeError(
                            f"bulk_write abandoned after {MAX_RETRIES + 1} "
                            f"attempts: {last_exc}"
                        ) from last_exc

            raise RuntimeError("Unexpected exit from retry loop.")

        # ── Producer: page through ALL Provider records ────────────────────
        def _producer() -> None:
            """
            Streams Provider documents in pages using keyset pagination on _id.
            Pushes lists of raw Provider dicts onto work_queue.
            Pushes one _QUEUE_DONE sentinel per consumer when finished or on error.
            """
            pages_produced: int = 0
            last_id: ObjectId | None = None
            try:
                while True:
                    query = (
                        {"_id": {"$gt": last_id}} if last_id is not None else {}
                    )
                    page: list[dict] = list(
                        self.ProviderCollection.find(query)
                        .sort("_id", pymongo.ASCENDING)
                        .limit(PAGE_SIZE)
                    )
                    if not page:
                        break   # no more documents
                    last_id = page[-1]["_id"]
                    work_queue.put(page)   # blocks if consumers are behind
                    pages_produced += 1
                    if pages_produced % 200 == 0:
                        logger.info(
                            "Producer: %d pages dispatched (~%d records so far).",
                            pages_produced, pages_produced * PAGE_SIZE,
                        )
            except Exception as exc:
                failure_info.append(f"Producer error: {exc}")
                logger.error("Producer failed: %s", exc)
            finally:
                # Signal every consumer to stop
                for _ in range(MAX_WORKERS):
                    work_queue.put(_QUEUE_DONE)
                logger.info("Producer finished after %d pages.", pages_produced)

        # ── Consumer: enrich a page and bulk-write it ──────────────────────
        def _consumer(worker_id: int) -> None:
            """
            Pulls pages from the work queue, enriches each Provider record
            using the in-memory _county_lookup, and bulk-writes in BATCH_SIZE
            chunks with retry.
            """
            nonlocal total_inserted
            local_buffer: list[dict] = []
            records_processed: int = 0

            def _flush_local() -> None:
                nonlocal total_inserted
                if not local_buffer:
                    return
                try:
                    inserted = _flush_batch(list(local_buffer))
                    local_buffer.clear()
                    with total_inserted_lock:
                        total_inserted += inserted
                except Exception as exc:
                    failure_info.append(
                        f"Worker {worker_id} flush error: {exc}"
                    )
                    logger.error("Worker %d flush failed: %s", worker_id, exc)
                    local_buffer.clear()
                    raise

            try:
                while True:
                    page = work_queue.get()
                    if page is _QUEUE_DONE:
                        break

                    for provider in page:
                        effective_zip = _extract_zip(provider)

                        # O(1) in-memory lookup – no DB query needed.
                        # ZIPs absent from the reference table return None
                        # via dict.get() default, which is correct: CountyInfo
                        # will be null for those providers.
                        county_info: dict | None = (
                            self._county_lookup.get(effective_zip)
                            if effective_zip
                            else None
                        )

                        enriched = copy.deepcopy(provider)
                        enriched.pop("_id", None)
                        enriched["CountyInfo"] = county_info
                        local_buffer.append(enriched)
                        records_processed += 1

                        if len(local_buffer) >= BATCH_SIZE:
                            _flush_local()

                # Flush any remaining docs after receiving the done sentinel
                _flush_local()

            except Exception as exc:
                failure_info.append(f"Worker {worker_id} fatal error: {exc}")
                logger.error("Worker %d exiting on error: %s", worker_id, exc)

            logger.info(
                "Worker %d done – processed %d records locally.",
                worker_id, records_processed,
            )

        # ── Launch producer + consumers ────────────────────────────────────
        producer_thread = threading.Thread(
            target=_producer, name="producer", daemon=True
        )
        consumer_threads = [
            threading.Thread(
                target=_consumer, args=(i,), name=f"consumer-{i}", daemon=True
            )
            for i in range(MAX_WORKERS)
        ]

        producer_thread.start()
        for t in consumer_threads:
            t.start()

        producer_thread.join()
        for t in consumer_threads:
            t.join()

        # ── Evaluate outcome ───────────────────────────────────────────────
        if failure_info:
            reasons = "; ".join(failure_info)
            raise RuntimeError(
                f"ProcessAllRecords failed after inserting {total_inserted} "
                f"record(s). Reasons: {reasons}"
            )

        logger.info(
            "ProcessAllRecords completed successfully. Total inserted: %d",
            total_inserted,
        )
        return True


