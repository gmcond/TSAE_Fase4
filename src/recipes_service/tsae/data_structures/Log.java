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
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable {
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private final ConcurrentHashMap<String, List<Operation>> log = new ConcurrentHashMap<>();

	public Log(List<String> participants){
		// create an empty log
    for (String participant : participants) {
      log.put(participant, new Vector<Operation>());
    }
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op){
			// Get the operation timestamp and the hostID
			Timestamp opTimestamp = op.getTimestamp();
			String hostId = opTimestamp.getHostid();

			List<Operation> operations = log.get(hostId);

			if (shouldAddOperation(operations, op)) {
				log.get(hostId).add(op);
			} else {
				Operation lastOp = operations.get(operations.size() - 1);
        return lastOp.getTimestamp().compare(op.getTimestamp()) < 0;
			}
		return true;
	}

	private boolean shouldAddOperation(List<Operation> operations, Operation operationToAdd) {
		if (operations.isEmpty()) {
			// if it is empty I will add the new operation
			return true;
		}

		// Otherwise, I will add it only if the operation timestamp is after the last one.
		Operation lastOperation = operations.get(operations.size() - 1);
		return operationToAdd.getTimestamp().compare(lastOperation.getTimestamp()) > 0;
	}

	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum){
		List<Operation> newOperations = new ArrayList<>();

		for (String hostId : this.log.keySet()) {
			List<Operation> operations = this.log.get(hostId);
			Timestamp last = operations.get(operations.size()-1).getTimestamp();

			Timestamp hostIdLastTimestamp = sum.getLast(hostId);

			if(last.compare(hostIdLastTimestamp) > 0) {
				if(hostIdLastTimestamp.isNullTimestamp()) {
					newOperations.addAll(operations);
				} else {
					for(Operation op : operations) {
						if (op.getTimestamp().compare(hostIdLastTimestamp) > 0)
							newOperations.add(op);
					}
				}
			}
		}

		return newOperations;
	}


	public synchronized List<Operation> msgList(String pid, Timestamp first)
	{
		List<Operation> messages = new Vector<>();

		for(Operation op : log.get(pid)) {
			if(op.getTimestamp().compare(first) > 0) {
				messages.add(op);
			}
		}

		return messages;

	}

	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary.
	 * @param ack: ackSummary.
	 */
	public synchronized void purgeLog(TimestampMatrix ack){
		TimestampVector minTSV = ack.minTimestampVector();
		for (String hostId : log.keySet()) {
			List<Operation> notToRemoveOperations = new ArrayList<>();
			Timestamp last = minTSV.getLast(hostId);

			if (!last.isNullTimestamp()) {
				for (Operation operation : log.get(hostId)) {
					if (!shouldPurgeOperation(hostId, operation, last)) {
						notToRemoveOperations.add(operation);
					}
				}
				log.put(hostId, notToRemoveOperations);
			}
		}
	}

	private boolean shouldPurgeOperation(String hostId, Operation operation, Timestamp last) {
		Timestamp opTimestamp = operation.getTimestamp();
		return last != null && last.compare(opTimestamp) < 0;
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Log)) {
			return false;
		}

		Log other = (Log) obj;
    if (log.size() != other.log.size()) {
      return false;
    }

		for (String hostId: log.keySet()) {
			if (!log.get(hostId).equals(other.log.get(hostId))) {
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
		StringBuilder name= new StringBuilder();
		for (Enumeration<List<Operation>> en=log.elements(); en.hasMoreElements(); ) {
			List<Operation> subLog=en.nextElement();
      for (Operation operation: subLog) {
        name.append(operation.toString()).append("\n");
      }
		}
		
		return name.toString();
	}
}
