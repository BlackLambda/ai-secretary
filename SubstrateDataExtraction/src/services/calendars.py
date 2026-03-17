"""
Calendar Service - Fetch calendar events and meetings.
"""

from typing import Dict

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json
from src.services.signals import get_signals
from datetime import datetime, timedelta
import json

meeting_fmt = "%Y-%m-%dT%H:%M:%S"

class CalendarService:
    """Service for fetching calendar and meeting data."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()
        
    
    def export_data_in_date_range(self, start_date: str, end_date: str):

        meetings_basic_info = self.get_events(start_date, end_date)
        meetings_recap = self.get_large_meetings_recap_by_timerage(start_date, end_date)
        meetings_transcripts = self.get_meeting_most_recent_transcripts(start_date, end_date)

        # Combine and save all data
        combined_data = {
            "meeting_basic_info": meetings_basic_info,
            "meetings_recap": meetings_recap,
            "meetings_transcripts": meetings_transcripts
        }

        return combined_data


    def get_events(self, start_date: str, end_date: str, top: int = 100) -> dict:
        """
        Fetch calendar events within a date range.

        Args:
            start_date: Start date in ISO format (e.g., "2025-01-01T00:00:00.000Z")
            end_date: End date in ISO format (e.g., "2025-01-31T23:59:59.999Z")
            top: Maximum number of events to fetch (default: 100)

        Returns:
            dict: {
                "count": N,
                "start_date": "...",
                "end_date": "...",
                "events": [...]
            }

        Saves to: output/calendar_events.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Calendar Events")
        print(f"From: {start_date}")
        print(f"To:   {end_date}")
        print(f"{'='*60}")

        url = "https://outlook.office.com/api/v2.0/me/calendarview"

        params = {
            "startdatetime": start_date,
            "enddatetime": end_date,
            "top": top
        }

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract events
        events = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(events),
            "start_date": start_date,
            "end_date": end_date,
            "events": events
        }

        # Save to file
        save_json(result, "calendar_events.json")

        print(f"{'='*60}\n")
        return result

    def get_meetings_recap_by_timerage(self, start_datetime: str, end_datetime: str, top: int = 10) -> Dict:
        """
        Fetch meetings recap within a date range.

        Args:
            start_datetime: Start datetime in ISO format (e.g., "2025-01-01T00:00:00.000Z")
            end_datetime: End datetime in ISO format (e.g., "2025-01-31T23:59:59.999Z")

        Returns:
            dict: Meetings recap data

        Saves to: output/meetings_recap.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Meetings Recap")
        print(f"From: {start_datetime}")
        print(f"To:   {end_datetime}")
        print(f"{'='*60}")

        url = "https://substrate.office.com/search/api/v1/recommendations?debug=0&cachebust=0&setflight=AITASKSV2,AINOTESV2,ReturnAllMeetings"

        request_body = {
            "EntityRequests":[
                {
                "QueryParameters": [
                    {
                    "EntityType": "MeetingCatchUp",
                    "Top": top,
                    "StartDateTime": "%sT00:00:00.000Z" % start_datetime,
                    "EndDateTime": "%sT23:59:59.999Z" % end_datetime
                    }
                ]
                }
            ],
            "Scenario": {
                "Name": "MeetingCatchUp"
            }
        }

        print(request_body)

        # Make request
        response_data = self.client.post(url, json=request_body)

        # Prepare output
        result = response_data

        # Save to file
        #save_json(result, f"meetings_recap——{start_datetime}——to——{end_datetime}.json")

        print(f"{'='*60}\n")
        return result

    def get_meetings_recap_by_calluid(self, calluid: str) -> Dict:
        """
        Fetch a specific meeting by its call UID.

        Args:
            calluid: The call UID

        Returns:
            dict: Meeting recap data

        Saves to: output/meetings_recap_{calluid}.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Meeting by Call UID: {calluid[:50]}...")
        print(f"{'='*60}")

        url = "https://substrate.office.com/search/api/v1/recommendations?debug=1&cachebust=0&setflight=AITASKSV2,AINOTESV2,ReturnAllMeetings"

        request_body = {
            "EntityRequests": [
                {
                    "Filter": {
                        "Term": {
                            "ICalUId": calluid
                        }
                    },
                    "QueryParameters": [
                        {
                        "EntityType": "MeetingCatchUp",
                        "Top": 3
                        }
                    ]
                }
            ],
            "Scenario": {
                "Name": "MeetingCatchUp"
            }
        }

        # Make request
        response_data = self.client.post(url, json=request_body)

         # Prepare output
        result = response_data

        # Save to file
        #save_json(result, f"meetings_recap_{calluid}.json")

        print(f"{'='*60}\n")

        return result

    
    def split_date_window(
        self,
        startdatetime: str,
        enddatetime: str,
        max_days: int = 1,
        inclusive: bool = True
    ):
        # Parse inputs
        try:
            start_date = datetime.strptime(startdatetime, "%Y-%m-%d").date()
            end_date = datetime.strptime(enddatetime, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Dates must be in 'yyyy-mm-dd' format, e.g., '2025-10-21'.")

        if max_days <= 0:
            raise ValueError("`max_days` must be a positive integer.")

        # Normalize end boundary based on inclusivity
        if inclusive:
            if start_date > end_date:
                raise ValueError("startdatetime must be <= enddatetime when inclusive=True.")
            # Each range can span up to `max_days` calendar days → end = start + (max_days - 1)
            step_span = timedelta(days=max_days - 1)
            ranges = []
            cur = start_date
            while cur <= end_date:
                sub_end = min(cur + step_span, end_date)
                ranges.append((cur.isoformat(), sub_end.isoformat()))
                cur = sub_end + timedelta(days=1)
            return ranges
        else:
            # Treat overall end as exclusive; allow start == end (results in empty list)
            if start_date > end_date:
                raise ValueError("startdatetime must be <= enddatetime (exclusive end allowed).")
            ranges = []
            cur = start_date
            # In exclusive mode, chunk length is up to `max_days` days, end is exclusive.
            while cur < end_date:
                sub_end_excl = min(cur + timedelta(days=max_days), end_date)
                ranges.append((cur.isoformat(), sub_end_excl.isoformat()))
                cur = sub_end_excl
    
        return ranges

    def get_large_meetings_recap_by_timerage(self, start_datetime: str, end_datetime: str, top: int = 100) -> Dict:
        time_intervals = self.split_date_window(start_datetime, end_datetime)

        final_results = {"EntitySets": [{"ResultSets":[{"Results": []}]}]}

        for time_interval in time_intervals:
            cur_results = self.get_meetings_recap_by_timerage(time_interval[0], time_interval[1], 10)
            if "EntitySets" in cur_results and cur_results["EntitySets"] is not None and len(cur_results["EntitySets"]) == 1 \
                and "ResultSets" in cur_results["EntitySets"][0] and cur_results["EntitySets"][0]["ResultSets"] is not None and len(cur_results["EntitySets"][0]["ResultSets"]) == 1 \
                and "Results" in cur_results["EntitySets"][0]["ResultSets"][0] and cur_results["EntitySets"][0]["ResultSets"][0]["Results"] is not None and len(cur_results["EntitySets"][0]["ResultSets"][0]["Results"]) >= 1:

                final_results["EntitySets"][0]["ResultSets"][0]["Results"].extend(cur_results["EntitySets"][0]["ResultSets"][0]["Results"])

        #save_json(final_results, "meetings_recap_merged.json")
        return final_results


    def get_meeting_most_recent_transcripts(self, start_datetime: str, end_datetime: str, top=500) -> dict:

        """
        Fetch most recent meeting transcripts.

        Args:
            start_datetime: Start datetime in ISO format (e.g., "2025-11-10T00:00:00.000Z")
            end_datetime: End datetime in ISO format (e.g., "2025-11-11T00:00:00.000Z")
            top: Maximum number of transcripts to fetch (default: 500)

        Returns:
            List of transcript objects
        """
        print(f"\n{'='*60}")
        print(f"Fetching Most Recent Meeting Transcripts")

        # Use the full datetime strings (remove milliseconds if present for API compatibility)
        start_time = start_datetime.replace('.000Z', 'Z') if '.000Z' in start_datetime else start_datetime
        end_time = end_datetime.replace('.000Z', 'Z') if '.000Z' in end_datetime else end_datetime

        url = f"https://substrate.office.com/api/beta/me/WorkingSetFiles?$select=FileName,FileExtension,FileContent/PrereleaseAnnotation, ItemProperties/Default/Created, ItemProperties/Default/MeetingRecording/MeetingRecordingTeamsThreadId, ItemProperties/Default/MeetingRecording/MeetingRecordingCallId,SharePointItem &$filter=(FileExtension eq 'mp4') and (FileCreatedTime ge {start_time} and FileCreatedTime lt {end_time}) &$orderby=FileCreatedTime desc"

        params = {
            "top": top
        }

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract events
        events = response_data.get('value', [])

        # Save to file
        #save_json(events, f"meeting_transcripts_recent_{top}.json")

        print(f"{'='*60}\n")
        return events

    def get_events_join_leave(self, start_date: str, end_date: str, top: int = 1000, token = "") -> dict:
        
        if token == "":
            token = self.client._token

        meeting_join_dat = get_signals(token, signal_types=["JoinCall"],
            since_utc_iso="%sT00:00:00Z" % start_date,
            end_utc_iso="%sT00:00:00Z" % end_date,
            top=top,
            host="https://substrate.office.com",
            api_version="v2.0"    # if you get a segment error, try beta first
            )

        meeting_leave_dat = get_signals(token, signal_types=["LeaveCall"],
            since_utc_iso="%sT00:00:00Z" % start_date,
            end_utc_iso="%sT00:00:00Z" % end_date,
            top=top,
            host="https://substrate.office.com",
            api_version="v2.0"    # if you get a segment error, try beta first
            )
        
        save_json({"meeting_join_dat": meeting_join_dat, "meeting_leave_dat": meeting_leave_dat}, f"meetings_activities.json")

        return {"meeting_join_dat": meeting_join_dat, "meeting_leave_dat": meeting_leave_dat}
    

    def get_my_previous_meeting_info(self, dat):

        final_results = []
        for i in range(len(dat['events'])):
            cur_item = dat['events'][i]

            iCalUId = cur_item['iCalUId']
            Uid = cur_item['Uid']
            Id = cur_item['Id']
            Subject = cur_item['Subject']
            Recurrence = cur_item['Recurrence']
            Organizer = cur_item['Organizer']
            Attendees = cur_item['Attendees']
            Start = cur_item['Start']
            End = cur_item['End']

            final_results.append(
			{
				"Id" : Id,
				"iCalUId": iCalUId,
				"Uid": Uid,
				"Subject": Subject,
				"Recurrence": Recurrence,
				"Organizer": Organizer,
				"Attendees": Attendees,
				"Start": Start,
				"End": End})

        return final_results
    
    def get_my_previous_meeting_relatd_activites(self, dat):
        final_results = []

        for i in range(len(dat)):
            cur_item = dat[i]

            ICalUID = cur_item['CustomProperties']['ICalUID'] if 'CustomProperties' in cur_item and 'ICalUID' in cur_item['CustomProperties'] else ""
            StartTime = cur_item['StartTime'] if "StartTime" in cur_item else ""
            EndTime = cur_item['EndTime'] if "EndTime" in cur_item else "" 
            final_results.append({"ICalUID": ICalUID, "StartTime":StartTime, "EndTime": EndTime})
    
        return final_results

    def get_meeting_recap(self, dat):
        final_results = []

        if dat is None or 'EntitySets' not in dat or dat['EntitySets'] is None or len(dat['EntitySets']) == 0 or 'ResultSets' not in dat['EntitySets'][0] or dat['EntitySets'][0]['ResultSets'] is None \
            or len(dat['EntitySets'][0]['ResultSets']) == 0 or 'Results' not in dat['EntitySets'][0]['ResultSets'][0]:
            return final_results

        for i in range(len(dat['EntitySets'][0]['ResultSets'][0]['Results'])):
            # i for meeting
            ICalUid = dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source']['MeetingRecording']['ICalUid']
            MeetingId = dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source']['MeetingId']

            speakers = {}

            if 'Source' not in dat['EntitySets'][0]['ResultSets'][0]['Results'][i] or 'Speakers' not in dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source'] or \
                dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source']['Speakers'] is None:
                pass
            else:
                for j in range(len(dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source']['Speakers'])):
                    speaker_name = dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source']['Speakers'][j]['DisplayName']
                    speaking_duration_str = dat['EntitySets'][0]['ResultSets'][0]['Results'][i]['Source']['Speakers'][j]['SpeakingOffsetIntervals']
                    intervals = speaking_duration_str.strip().split(";")
                    speaking_duration = sum(float(pair.split(',')[1]) for pair in intervals if pair)

                    speakers[speaker_name] = {"speaker_name": speaker_name, "speaking_duration": speaking_duration}
                    print(speaker_name, speaking_duration)

            final_results.append({"ICalUid":ICalUid, "MeetingId":MeetingId, "speakers": speakers})

        return final_results
    
    def merge_all_meeting_info(self, meeting_basic_info, meeting_join_info, meeting_leave_info, meeting_recap, mailboxowner_displayname = "Xiangyang Zhou"):

        final_result = {}

        for meeting_instance in meeting_basic_info:
            iCalUId = meeting_instance['iCalUId']
            meetingId = meeting_instance['Id']
            meetingSubject = meeting_instance['Subject']
            meetingRecurrence = meeting_instance['Recurrence']
            meetingOrganizer = meeting_instance['Organizer']
            meetingStartDateTime = meeting_instance['Start']['DateTime']
            meetingEndDateTime = meeting_instance['End']['DateTime']
            meetingAttendees = {}
            for person in meeting_instance['Attendees']:
                displayName = person['EmailAddress']['Name']
                emailAddress = person['EmailAddress']['Address']
                isRequired = person['Type']

                meetingAttendees[emailAddress] = {"displayName": displayName, "emailAddress":emailAddress, "isRequired":isRequired, "speakingTime":-1.0}

            startTime = datetime.strptime(meetingStartDateTime[0:len("2025-09-11T15:30:00")], meeting_fmt)
            endTime = datetime.strptime(meetingEndDateTime[0:len("2025-09-11T15:30:00")], meeting_fmt)
            meetingDuration= abs((endTime-startTime).total_seconds() / 60)

            meetingJoinDatetimeByMailboxOwner = ""
            meetingLeaveDatetimeByMailboxOwner = ""

            for join_activity in meeting_join_info:
                if join_activity['ICalUID'] == iCalUId:
                    if meetingJoinDatetimeByMailboxOwner == "":
                        meetingJoinDatetimeByMailboxOwner = join_activity['StartTime']
                    else:
                        meetingJoinDatetimeByMailboxOwner = min(meetingJoinDatetimeByMailboxOwner, join_activity['StartTime'])

            for leave_activity in meeting_leave_info:
                if leave_activity['ICalUID'] == iCalUId:
                    if meetingLeaveDatetimeByMailboxOwner == "":
                        meetingLeaveDatetimeByMailboxOwner = leave_activity['StartTime']
                    else:
                        meetingLeaveDatetimeByMailboxOwner = max(meetingLeaveDatetimeByMailboxOwner, leave_activity['StartTime'])

            if meetingId in final_result:
                print("[Warning] found in duplicated meeting %s" % meetingId)
            else:
                final_result[meetingId] = {
                    "iCalUId" : iCalUId,
                    "meetingId": meetingId,
                    "meetingSubject": meetingSubject,
                    "meetingRecurrence": meetingRecurrence,
                    "meetingOrganizer": meetingOrganizer,
                    "meetingStartDateTime": meetingStartDateTime,
				    "meetingEndDateTime": meetingEndDateTime,
				    "meetingDuration": meetingDuration,
				    "meetingJoinDatetimeByMailboxOwner": meetingJoinDatetimeByMailboxOwner,
				    "meetingLeaveDatetimeByMailboxOwner": meetingLeaveDatetimeByMailboxOwner,
				    "meetingAttendees": meetingAttendees
			    }

        for meeting_recap_instance in meeting_recap:
		# {"ICalUid":ICalUid, "MeetingId":MeetingId, "speakers": speakers}
            if meeting_recap_instance['MeetingId'] in final_result:
                for person_email in final_result[meeting_recap_instance['MeetingId']]["meetingAttendees"]:
                    person = final_result[meeting_recap_instance['MeetingId']]["meetingAttendees"][person_email]
                    for speaker_displayname in meeting_recap_instance['speakers']:
					    # {"speaker_name": speaker_name, "speaking_duration": speaking_duratio
                        if speaker_displayname == person["displayName"]:
                            final_result[meeting_recap_instance['MeetingId']]["meetingAttendees"][person_email]['speakingTime'] = round(meeting_recap_instance['speakers'][speaker_displayname]['speaking_duration'] / 60)

        # back fall 
        for meeting_instance_id in final_result:
            meeting_instance = final_result[meeting_instance_id]
            for attendee_email in meeting_instance['meetingAttendees']:
                attendee = meeting_instance['meetingAttendees'][attendee_email]

                if attendee['displayName'] == mailboxowner_displayname and attendee['speakingTime'] > 0:
                    if final_result[meeting_instance_id]['meetingJoinDatetimeByMailboxOwner'] == "" and final_result[meeting_instance_id]['meetingLeaveDatetimeByMailboxOwner'] == "":
                        final_result[meeting_instance_id]['meetingJoinDatetimeByMailboxOwner'] = final_result[meeting_instance_id]['meetingStartDateTime']
                        final_result[meeting_instance_id]['meetingLeaveDatetimeByMailboxOwner'] = final_result[meeting_instance_id]['meetingEndDateTime']
	
        if final_result[meeting_instance_id]['meetingJoinDatetimeByMailboxOwner'] == "" and final_result[meeting_instance_id]['meetingLeaveDatetimeByMailboxOwner'] != "":
            final_result[meeting_instance_id]['meetingJoinDatetimeByMailboxOwner'] = final_result[meeting_instance_id]['meetingStartDateTime']
        elif final_result[meeting_instance_id]['meetingJoinDatetimeByMailboxOwner'] != "" and final_result[meeting_instance_id]['meetingLeaveDatetimeByMailboxOwner'] == "":
            final_result[meeting_instance_id]['meetingLeaveDatetimeByMailboxOwner'] = final_result[meeting_instance_id]['meetingEndDateTime']


        for meetingId in final_result:
            if final_result[meetingId]['meetingJoinDatetimeByMailboxOwner'] != "" or final_result[meetingId]['meetingLeaveDatetimeByMailboxOwner'] != "":
                print(json.dumps(final_result[meetingId], indent=4))

        return final_result

if __name__ == "__main__":
    
    test_client = CalendarService()

    test_client.get_events("2025-10-17", "2025-10-23")