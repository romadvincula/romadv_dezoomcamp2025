from typing import Dict


class RideRecordKey:
    """Message key for streaming ride data - determines which partition receives the message"""
    def __init__(self, vendor_id):
        # Store vendor ID as the partitioning key for distributed streaming
        self.vendor_id = vendor_id

    @classmethod
    def from_dict(cls, d: Dict):
        # Create key object from dictionary data (used in deserialization)
        return cls(vendor_id=d['vendor_id'])

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride_record_key(obj, ctx):
    """Convert dictionary to RideRecordKey - used by Avro deserializer"""
    if obj is None:
        return None

    return RideRecordKey.from_dict(obj)


def ride_record_key_to_dict(ride_record_key: RideRecordKey, ctx):
    """Convert RideRecordKey to dictionary - used by Avro serializer"""
    return ride_record_key.__dict__