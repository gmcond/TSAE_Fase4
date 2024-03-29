/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable {
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();
	
	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private final ConcurrentHashMap<String, Timestamp> timestampVector = new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		for (String id: participants) {
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		LSimLogger.log(Level.TRACE, "Updating the TimestampVectorInserting with the timestamp: " + timestamp);
		this.timestampVector.replace(timestamp.getHostid(), timestamp);
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector) {
		for (String key : this.timestampVector.keySet()) {
			Timestamp currentHostTimestamp = this.getLast(key);
			Timestamp newTimestamp = tsVector.getLast(key);
			if (isNewerTimestamp(newTimestamp, currentHostTimestamp)) {
				updateTimestamp(newTimestamp);
			}
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public Timestamp getLast(String node) {
		return this.timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		for (String key: tsVector.timestampVector.keySet()) {
			Timestamp otherHostTimestamp = tsVector.timestampVector.get(key);
			Timestamp currentTimestamp = this.timestampVector.get(key);
			if(isOlderTimestamp(otherHostTimestamp, currentTimestamp)) {
				updateTimestamp(otherHostTimestamp);
			}
		}
	}

	private boolean isNewerTimestamp(Timestamp timestamp, Timestamp newerThan) {
		return timestamp.compare(newerThan) > 0;
	}

	private boolean isOlderTimestamp(Timestamp timestamp, Timestamp olderThan) {
		return timestamp.compare(olderThan) < 0;
	}
	
	/**
	 * clone
	 */
	public TimestampVector clone(){
		List<String> participants = new ArrayList<>(timestampVector.keySet());
		TimestampVector clonedVector = new TimestampVector(participants);

		for (String key : timestampVector.keySet()) {
			Timestamp ts = this.timestampVector.get(key);
			clonedVector.timestampVector.put(ts.getHostid(), ts);
		}
		return clonedVector;
	}
	
	/**
	 * equals
	 */
	public boolean equals(Object obj){
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof TimestampVector)) {
			return false;
		}
		TimestampVector otherTimestampVector = (TimestampVector) obj;
    if (timestampVector.size() != otherTimestampVector.timestampVector.size()) {
      return false;
    }
    for (String hostId: timestampVector.keySet()) {
      if (!timestampVector.get(hostId).equals(otherTimestampVector.timestampVector.get(hostId))) {
        return false;
      }
    }
    return true;
  }

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		StringBuilder all = new StringBuilder();
    for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all.append(timestampVector.get(name)).append("\n");
		}
		return all.toString();
	}
}
