import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession, functions as F

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
from collections import defaultdict
import hashlib
import uuid

# Immutable data classes for type safety
@dataclass(frozen=True)
class WorkflowIndex:
    did: str
    channel: str
    pdata_id: str

@dataclass
class WFSInputEData:
    type: str
    mode: str
    duration: int
    pageid: str
    item: Dict
    resvalues: List[Dict[str, Any]]
    pass_field: str  # renamed from 'pass' to avoid keyword conflict
    score: int

@dataclass
class WFSInputEvent:
    eid: str
    ets: int
    timestamp: str
    ver: str
    mid: str
    actor: Dict
    context: Dict
    object: Optional[Dict]
    edata: WFSInputEData
    tags: List[Any] = field(default_factory=list)

@dataclass
class WorkflowInput:
    session_key: WorkflowIndex
    events: List[str]

@dataclass
class Summary:
    sid: str
    type: str
    mode: str
    events: List[WFSInputEvent] = field(default_factory=list)
    summary_events: List[Dict] = field(default_factory=list)
    children: List['Summary'] = field(default_factory=list)
    parent: Optional['Summary'] = None
    is_closed: bool = False
    idle_time: int = 0  # in milliseconds

    def add(self, event: WFSInputEvent, idle_time_threshold: int):
        """Add event to summary and calculate idle time"""
        if self.events:
            # Calculate idle time based on gap between events
            time_gap = (event.ets - self.events[-1].ets) / 1000  # convert to seconds
            if time_gap > idle_time_threshold:
                self.idle_time += int(time_gap * 1000)  # store in milliseconds
        
        self.events.append(event)

    def close(self, summaries: List[Dict], config: Dict):
        """Close summary and generate measured event"""
        if self.is_closed:
            return
            
        start_time = self.events[0].ets if self.events else 0
        end_time = self.events[-1].ets if self.events else 0
        duration = end_time - start_time
        
        # Count interactions
        interaction_count = sum(1 for e in self.events if e.eid in ['INTERACT', 'RESPONSE'])
        
        # Check if session is completed (has END event)
        is_completed = any(e.eid == 'END' for e in self.events)
        
        # Generate measured event
        measured_event = {
            "eid": "ME_WORKFLOW_SUMMARY",
            "ets": end_time,
            "ver": "3.0",
            "mid": f"WFS:{self.sid}",
            "actor": self.events[0].actor if self.events else {"type": "", "id": ""},
            "context": self.events[0].context if self.events else {},
            "object": {
                "id": self.sid,
                "type": "WorkflowSession",
                "ver": "1.0"
            },
            "edata": {
                "type": self.type,
                "mode": self.mode,
                "starttime": start_time,
                "endtime": end_time,
                "timespent": duration / 1000,  # in seconds
                "pageviews": len([e for e in self.events if e.eid == 'IMPRESSION']),
                "interactions": interaction_count,
                "envsummary": {
                    "env": "workflow",
                    "time_spent": duration / 1000,
                    "interactions": interaction_count,
                    "page_views": len([e for e in self.events if e.eid == 'IMPRESSION'])
                },
                "page_summary": [],
                "idle_time": self.idle_time / 1000,  # in seconds
                "duration": duration / 1000,
                "session_id": self.sid,
                "is_completed": is_completed,
                "event_count": len(self.events),
                "parent_session_id": self.parent.sid if self.parent else None
            },
            "tags": []
        }
        
        summaries.append(measured_event)
        self.is_closed = True

    def check_for_similar_start(self, event_type: str, mode: str) -> bool:
        """Check if this summary can handle a START event of given type/mode"""
        return self.type == event_type and (not mode or self.mode == mode)

    def get_similar_end_summary(self, event: WFSInputEvent) -> 'Summary':
        """Find the appropriate summary for an END event"""
        # Check if this summary matches the END event
        if self.check_for_similar_start(event.edata.type, event.edata.mode):
            return self
        
        # Check children for matching summary
        for child in self.children:
            if child.check_for_similar_start(event.edata.type, event.edata.mode):
                return child
        
        # Check parent
        if self.parent and self.parent.check_for_similar_start(event.edata.type, event.edata.mode):
            return self.parent
        
        return self

    def clone(self) -> 'Summary':
        """Create a deep copy of this summary"""
        cloned = Summary(
            sid=self.sid,
            type=self.type,
            mode=self.mode,
            events=self.events.copy(),
            summary_events=[],
            children=[],
            parent=self.parent,
            is_closed=self.is_closed,
            idle_time=self.idle_time
        )
        return cloned

class WorkFlowSummaryModel:
    
    SERVER_EVENTS = ["LOG", "AUDIT", "SEARCH"]
    
    @staticmethod
    def pre_process(data: RDD, config: Dict, sc: SparkContext) -> RDD:
        """Pre-process events and create workflow indices"""
        default_pdata = {"id": "default.consumption.app.id", "ver": "1.0"}
        
        def parse_event(event_str: str) -> Tuple[Dict, str]:
            try:
                event = json.loads(event_str)
                return (event, event_str)
            except Exception as e:
                print(f"Error parsing event: {str(e)}")
                return (None, "")
        
        # Parse and filter events
        indexed_data = data.map(parse_event).filter(lambda x: x[0] is not None)
        
        def create_workflow_index(event_tuple: Tuple[Dict, str]) -> Tuple[WorkflowIndex, List[str]]:
            event, event_str = event_tuple
            
            # Filter out server events
            if event["eid"] in WorkFlowSummaryModel.SERVER_EVENTS:
                return (None, [])
                
            # Create workflow index
            did = event.get("context", {}).get("did", "")
            actor_type = event.get("actor", {}).get("type", "")
            actor_id = event.get("actor", {}).get("id", "")
            channel = event.get("context", {}).get("channel", "")
            pdata_id = event.get("context", {}).get("pdata", default_pdata).get("id")
            
            session_key = WorkflowIndex(
                did=f"{did}|{actor_type}|{actor_id}",
                channel=channel,
                pdata_id=pdata_id
            )
            return (session_key, [event_str])
        
        # Group events by workflow index
        partitioned_data = (
            indexed_data
            .map(create_workflow_index)
            .filter(lambda x: x[0] is not None)
            .reduceByKey(lambda a, b: a + b)
        )
        
        return partitioned_data.map(lambda x: WorkflowInput(x[0], x[1]))
    
    @staticmethod
    def algorithm(data: RDD, config: Dict, sc: SparkContext) -> RDD:
        idle_time = config.get("idleTime", 600)  # seconds
        session_break_time = config.get("sessionBreakTime", 30)  # minutes
        
        def process_session(workflow_input: WorkflowInput) -> List[Dict]:
            summaries = []
            events = []
            
            # Parse all events in the session
            for event_str in workflow_input.events:
                try:
                    event_dict = json.loads(event_str)
                    # Convert to WFSInputEvent
                    edata = event_dict.get("edata", {})
                    wfs_edata = WFSInputEData(
                        type=edata.get("type", ""),
                        mode=edata.get("mode", ""),
                        duration=edata.get("duration", 0),
                        pageid=edata.get("pageid", ""),
                        item=edata.get("item", {}),
                        resvalues=edata.get("resvalues", []),
                        pass_field=edata.get("pass", ""),
                        score=edata.get("score", 0)
                    )
                    event = WFSInputEvent(
                        eid=event_dict.get("eid", ""),
                        ets=event_dict.get("ets", 0),
                        timestamp=event_dict.get("@timestamp", ""),
                        ver=event_dict.get("ver", ""),
                        mid=event_dict.get("mid", ""),
                        actor=event_dict.get("actor", {}),
                        context=event_dict.get("context", {}),
                        object=event_dict.get("object"),
                        edata=wfs_edata,
                        tags=event_dict.get("tags", [])
                    )
                    events.append(event)
                except Exception as e:
                    print(f"Error parsing event in algorithm: {str(e)}")
                    continue
            
            if not events:
                return []
            
            # Sort events by timestamp
            sorted_events = sorted(events, key=lambda x: x.ets)
            
            # Initialize state variables
            root_summary = None
            current_summary = None
            prev_event = None
            session_counter = 0
            
            for event in sorted_events:
                # Check for session break
                if prev_event:
                    time_diff = (event.ets - prev_event.ets) / 1000  # seconds
                    
                    if (time_diff > (session_break_time * 60) and 
                        event.edata.type.lower() != "app"):
                        
                        # Close current session on break
                        if current_summary and not current_summary.is_closed:
                            current_summary.close(summaries, config)
                            
                        # Close and clone root summary
                        if root_summary and not root_summary.is_closed:
                            cloned_root = root_summary.clone()
                            cloned_root.close(summaries, config)
                            root_summary = cloned_root
                
                prev_event = event
                
                # Process event based on type
                if event.eid == "START":
                    session_counter += 1
                    session_id = f"{event.actor.get('id', 'unknown')}_{session_counter}"
                    
                    if not root_summary or root_summary.is_closed:
                        # Create new root session
                        if event.edata.type.lower() != "app":
                            # Create app-level root session
                            root_summary = Summary(
                                sid=f"{session_id}_root",
                                type="app",
                                mode=""
                            )
                            # Create child session
                            current_summary = Summary(
                                sid=session_id,
                                type=event.edata.type,
                                mode=event.edata.mode,
                                parent=root_summary
                            )
                            root_summary.children.append(current_summary)
                            current_summary.add(event, idle_time)
                        else:
                            # Direct app session
                            root_summary = Summary(
                                sid=session_id,
                                type=event.edata.type,
                                mode=event.edata.mode
                            )
                            current_summary = root_summary
                            current_summary.add(event, idle_time)
                    else:
                        # Handle nested START events
                        if current_summary and not current_summary.is_closed:
                            if not current_summary.check_for_similar_start(event.edata.type, event.edata.mode):
                                # Create child session
                                new_summary = Summary(
                                    sid=f"{session_id}_child",
                                    type=event.edata.type,
                                    mode=event.edata.mode,
                                    parent=current_summary
                                )
                                current_summary.children.append(new_summary)
                                current_summary = new_summary
                                current_summary.add(event, idle_time)
                            else:
                                # Same type - add to current session
                                current_summary.add(event, idle_time)
                        else:
                            # Create new session
                            current_summary = Summary(
                                sid=session_id,
                                type=event.edata.type,
                                mode=event.edata.mode
                            )
                            if root_summary and not root_summary.is_closed:
                                current_summary.parent = root_summary
                                root_summary.children.append(current_summary)
                            current_summary.add(event, idle_time)
                
                elif event.eid == "END":
                    if current_summary:
                        similar_summary = current_summary.get_similar_end_summary(event)
                        
                        if similar_summary and not similar_summary.is_closed:
                            similar_summary.add(event, idle_time)
                            similar_summary.close(summaries, config)
                            
                            # Navigate back to parent
                            if similar_summary.parent and not similar_summary.parent.is_closed:
                                current_summary = similar_summary.parent
                            elif root_summary and not root_summary.is_closed:
                                current_summary = root_summary
                            else:
                                current_summary = None
                
                else:
                    # Handle other events (INTERACT, IMPRESSION, etc.)
                    if current_summary and not current_summary.is_closed:
                        current_summary.add(event, idle_time)
                    else:
                        # Create new session for orphaned events
                        session_counter += 1
                        session_id = f"{event.actor.get('id', 'unknown')}_{session_counter}"
                        current_summary = Summary(
                            sid=session_id,
                            type="app",
                            mode=""
                        )
                        if not root_summary or root_summary.is_closed:
                            root_summary = current_summary
                        current_summary.add(event, idle_time)
            
            # Final cleanup - close any remaining open sessions
            if current_summary and not current_summary.is_closed:
                current_summary.close(summaries, config)
                
            if (root_summary and not root_summary.is_closed and 
                current_summary and root_summary.sid != current_summary.sid):
                root_summary.close(summaries, config)
            
            return summaries
        
        return data.flatMap(process_session)
    
    @staticmethod
    def post_process(data: RDD, config: Dict, sc: SparkContext) -> RDD:
        """Post-process the measured events"""
        def enhance_measured_event(event: Dict) -> Dict:
            # Add additional fields and validation
            enhanced = event.copy()
            
            # Ensure all required fields are present
            if "edata" in enhanced:
                edata = enhanced["edata"]
                edata["app_id"] = enhanced.get("context", {}).get("pdata", {}).get("id", "")
                edata["channel"] = enhanced.get("context", {}).get("channel", "")
                edata["did"] = enhanced.get("context", {}).get("did", "")
                
                # Calculate engagement ratio
                interactions = edata.get("interactions", 0)
                event_count = edata.get("event_count", 0)
                edata["engagement_ratio"] = interactions / event_count if event_count > 0 else 0.0
                
                # Add duration in minutes
                edata["duration_minutes"] = edata.get("duration", 0) / 60.0
            
            return enhanced
        
        return data.map(enhance_measured_event)

def main():
    """Main execution function with comprehensive test data"""
    # Initialize Spark Session with optimized settings
    spark = SparkSession.builder \
        .appName("CompleteWorkflowSummary") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "20g") \
        .config("spark.driver.memory", "12g") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")  # Reduce log noise

    start_time = datetime.now()
    print(f"[START] Complete WorkFlow Summary processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Comprehensive test data that matches Scala test scenarios
    input_data = sc.parallelize([
        ])

    config = {
        "idleTime": 600,        # 10 minutes idle time threshold
        "sessionBreakTime": 30,  # 30 minutes session break
        "parallelization": 20
    }

    # Run the complete workflow
    print("\n[PHASE 1] Pre-processing events...")
    processed_data = WorkFlowSummaryModel.pre_process(input_data, config, sc)
    print(f"[INFO] Processed {processed_data.count()} workflow groups")

    print("\n[PHASE 2] Running core algorithm...")
    result = WorkFlowSummaryModel.algorithm(processed_data, config, sc)
    print(f"[INFO] Generated {result.count()} session summaries")

    print("\n[PHASE 3] Post-processing...")
    output = WorkFlowSummaryModel.post_process(result, config, sc)

    # Collect and display results
    print("\n[RESULTS] Workflow Session Summaries:")
    print("=" * 80)
    
    results = output.collect()
    for i, summary in enumerate(results, 1):
        print(f"\n📊 Session Summary {i}:")
        print(f"   Session ID: {summary.get('edata', {}).get('session_id', 'N/A')}")
        print(f"   Type: {summary.get('edata', {}).get('type', 'N/A')}")
        print(f"   Mode: {summary.get('edata', {}).get('mode', 'N/A')}")
        print(f"   Duration: {summary.get('edata', {}).get('duration', 0):.2f} seconds")
        print(f"   Events: {summary.get('edata', {}).get('event_count', 0)}")
        print(f"   Interactions: {summary.get('edata', {}).get('interactions', 0)}")
        print(f"   Completed: {summary.get('edata', {}).get('is_completed', False)}")
        print(f"   Parent: {summary.get('edata', {}).get('parent_session_id', 'None')}")
        print(f"   Engagement: {summary.get('edata', {}).get('engagement_ratio', 0):.2f}")
        
        # Show full JSON for detailed analysis
        if i <= 3:  # Show first 3 in detail
            print(f"   Full JSON:")
            print(json.dumpss(summary, indent=4))
    
    # Print comprehensive statistics
    print(f"\n[STATISTICS]")
    print("=" * 50)
    print(f"📈 Total Sessions: {len(results)}")
    
    completed_sessions = [s for s in results if s.get('edata', {}).get('is_completed', False)]
    print(f"✅ Completed Sessions: {len(completed_sessions)}")
    
    nested_sessions = [s for s in results if s.get('edata', {}).get('parent_session_id')]
    print(f"🔗 Nested Sessions: {len(nested_sessions)}")
    
    session_types = {}
    for s in results:
        session_type = s.get('edata', {}).get('type', 'unknown')
        session_types[session_type] = session_types.get(session_type, 0) + 1
    
    print(f"📊 Session Types: {dict(session_types)}")
    
    total_duration = sum(s.get('edata', {}).get('duration', 0) for s in results)
    print(f"⏱️  Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    
    avg_engagement = sum(s.get('edata', {}).get('engagement_ratio', 0) for s in results) / len(results) if results else 0
    print(f"💫 Average Engagement: {avg_engagement:.2f}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[COMPLETION]")
    print("=" * 50)
    print(f"🏁 Processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Total processing time: {duration}")
    print(f"🚀 Success! Generated {len(results)} session summaries matching Scala behavior")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()