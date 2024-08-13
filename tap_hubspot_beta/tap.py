"""hubspot tap class."""

from typing import List, cast

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from typing import List, Type, Dict
from singer_sdk.helpers._compat import final


from tap_hubspot_beta.streams import (
    AccountStream,
    AssociationDealsCompaniesStream,
    AssociationDealsContactsStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    CallsStream,
    CompaniesStream,
    ContactEventsStream,
    ContactListData,
    ContactListsStream,
    ContactsStream,
    DealsStream,
    EmailEventsStream,
    EmailsStream,
    FormsStream,
    FormSubmissionsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    ListsStream,
    MeetingsStream,
    NotesStream,
    OwnersStream,
    ProductsStream,
    TasksStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    CurrenciesStream,
    ContactsAssociationsStream,
    MeetingsAssociationsStream,
    CallsAssociationsStream,
    CommunicationsAssociationsStream,
    EmailsAssociationsStream,
    NotesAssociationsStream,
    PostalMailAssociationsStream,
    TasksAssociationsStream,
    QuotesAssociationsStream,
    ProductsAssociationsStream,
    TicketsAssociationsStream,
    CompaniesAssociationsStream,
    ContactsAssociatedRecords,
    QuotesAssociatedRecords,
    ProductsAssociatedRecords,
    TicketsAssociatedRecords,
    CompaniesAssociatedRecords,
    TasksAssociatedRecords,
    PostalMailAssociatedRecords,
    NotesAssociatedRecords,
    EmailsAssociatedRecords,
    CommunicationsAssociatedRecords,
    CallsAssociatedRecords,
    MeetingsAssociatedRecords
)

STREAM_TYPES = [
    ContactsStream,
    ListsStream,
    CompaniesStream,
    DealsStream,
    OwnersStream,
    ContactListsStream,
    ContactListData,
    ProductsStream,
    LineItemsStream,
    # ArchivedLineItemsStream,
    # AssociationDealsContactsStream,
    # AssociationDealsCompaniesStream,
    # AssociationDealsLineItemsStream,
    # AssociationContactsCompaniesStream,
    # AssociationContactsTicketsStream,
    AccountStream,
    FormsStream,
    FormSubmissionsStream,
    ContactEventsStream,
    EmailEventsStream,
    EmailsStream,
    NotesStream,
    MeetingsStream,
    TasksStream,
    CallsStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    # AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    # ArchivedCompaniesStream,
    # ArchivedDealsStream,
    # DealsAssociationParent,
    CurrenciesStream,
    ContactsAssociationsStream,
    MeetingsAssociationsStream,
    CallsAssociationsStream,
    CommunicationsAssociationsStream,
    EmailsAssociationsStream,
    NotesAssociationsStream,
    PostalMailAssociationsStream,
    TasksAssociationsStream,
    QuotesAssociationsStream,
    ProductsAssociationsStream,
    TicketsAssociationsStream,
    CompaniesAssociationsStream,
    ContactsAssociatedRecords,
    QuotesAssociatedRecords,
    ProductsAssociatedRecords,
    TicketsAssociatedRecords,
    CompaniesAssociatedRecords,
    TasksAssociatedRecords,
    PostalMailAssociatedRecords,
    NotesAssociatedRecords,
    EmailsAssociatedRecords,
    CommunicationsAssociatedRecords,
    CallsAssociatedRecords,
    MeetingsAssociatedRecords
]


class Taphubspot(Tap):
    """hubspot tap class."""

    name = "tap-hubspot"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)
    
    fetched_objects_ids = {}

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("expires_in", th.IntegerType),
        th.Property("start_date", th.DateTimeType),
        th.Property("access_token", th.StringType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """

        # add visible field to metadata
        original_catalog = self._singer_catalog
        # for some reason to_dict() gets rid of the value visible so we'll add it back before json dumping the catalog
        _catalog = self._singer_catalog.to_dict()
        new_catalog = []
        for i, stream in enumerate(_catalog["streams"]):
            stream["metadata"][-1]["metadata"]["visible"] = original_catalog[stream["tap_stream_id"]].metadata[()].visible
            new_catalog.append(stream)
        catalog =  cast(dict, {"streams": new_catalog})

        # add metadata fields to catalog
        if self.config.get("catalog_metadata", False):
            streams = self.streams
            for stream in catalog["streams"]:
                stream_class = streams[stream["tap_stream_id"]]
                stream["stream_meta"] = {}
                if hasattr(stream_class, "load_fields_metadata"):
                    stream_class.load_fields_metadata()
                    for field in stream["schema"]["properties"]:
                        stream["schema"]["properties"][field]["field_meta"] = stream_class.fields_metadata.get(field, {})
        return catalog

    @property
    def streams(self) -> Dict[str, Stream]:
        """Get streams discovered or catalogued for this tap.

        Results will be cached after first execution.

        Returns:
            A mapping of names to streams, using discovery or a provided catalog.
        """
        input_catalog = self.input_catalog

        if self._streams is None:
            self._streams = {}
            for stream in self.load_streams():
                if input_catalog is not None:
                    stream.apply_catalog(input_catalog)
                
                # add metadata visible value
                setattr(stream.metadata[()], "visible", stream.visible_in_catalog)
                self._streams[stream.name] = stream
        return self._streams
    
    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )
        
        # clean streams dict and use stream name as keys
        streams_by_type = {v[0].name:v[0] for v in streams_by_type.values()}


        # change name for v3 streams and hide streams with the same base name (personalized request HGI-5253)
        del_streams = []
        if not self.config.get("use_legacy_streams", True):
            for stream in streams_by_type.values():
                if "_v3" in stream.name:
                    # if stream name has v3 in it remove 'v3' from the stream name 
                    base_name = stream.name.split("_v3")[0]
                    # excluding streams with the same base name from the catalog
                    if base_name in streams_by_type.keys():
                        # if same name stream is a parent stream change name and change visible = False
                        if streams_by_type[base_name].child_streams:
                            streams_by_type[base_name].name = f"is_parent_stream_{base_name}"
                            streams_by_type[base_name].visible_in_catalog = False
                        else:
                            del_streams.append(base_name)
                    # change v3 name to base_name
                    stream.name = base_name
        
        if not self.config.get("use_list_selection", False):
            del_streams = del_streams + ["contact_list", "contact_list_data"]
        
        # delete streams that have same name as v3 stream
        streams_by_type = {key: value for key, value in streams_by_type.items() if key not in del_streams}

        streams = streams_by_type.values()
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )

    @final
    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        # force dynamic associations streams to be synced at the end:
        associations_stream = {stream_name: stream for stream_name, stream in self.streams.items() if ("_associations" in stream_name or "_associated_records" in stream_name)}
        # pop associations streams from self.streams to order them later
        [self.streams.pop(stream_name) for stream_name in associations_stream]
        # order self.streams to sync associations at the end
        self.streams.update(associations_stream)

        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()


if __name__ == "__main__":
    Taphubspot.cli()
