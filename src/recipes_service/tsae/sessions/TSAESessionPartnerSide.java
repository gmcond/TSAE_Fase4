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

package recipes_service.tsae.sessions;


import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread {
	
	private final Socket socket;
	private final ServerData serverData;
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {
		Message msg;

		int currentSessionNumber = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			TimestampVector localSummary;
			TimestampMatrix localAck;
			// receive request from originator and update local state
			synchronized (serverData) {
				localSummary = serverData.getSummary().clone();
				serverData.getAck().update(serverData.getId(), localSummary);
				localAck = serverData.getAck().clone();
			}

			// receive originator's summary and ack
			msg = (Message) in.readObject();
			currentSessionNumber = msg.getSessionNumber();
			//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] TSAE session");
			//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] received message: "+ msg);
			if (msg.type() == MsgType.AE_REQUEST) {
				MessageAErequest originator = (MessageAErequest) msg;
				List<Operation> operations = serverData.getLog().listNewer(originator.getSummary());
				
				// send operations
				for (Operation op: operations) {
					MessageOperation operationMsg = new MessageOperation(op);
					operationMsg.setSessionNumber(currentSessionNumber);
					out.writeObject(operationMsg);
					//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] sent message: " + operationMsg);
				}

				// send to originator: local's summary and ack
				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(currentSessionNumber);
				out.writeObject(msg);
				//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] sent message: "+ msg);

				// receive operations
				msg = (Message) in.readObject();
				//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] received message: "+ msg);

				while (msg.type() == MsgType.OPERATION){
					MessageOperation msgOperation = (MessageOperation) msg;
					synchronized (serverData) {
						serverData.performOperation(msgOperation.getOperation());
					}
					msg = (Message) in.readObject();
					//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] received message: "+ msg);
				}
				
				// receive message to inform about the ending of the TSAE session
				if (msg.type() == MsgType.END_TSAE){
					// send and "end of TSAE session" message
					MessageEndTSAE endTSAEMsg = new MessageEndTSAE();
					endTSAEMsg.setSessionNumber(currentSessionNumber);
					out.writeObject(endTSAEMsg);
					//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] sent message: "+ msg);
					synchronized (serverData) {
						serverData.getSummary().updateMax(originator.getSummary());
						serverData.getAck().updateMax(originator.getAck());
						serverData.getLog().purgeLog(serverData.getAck());
					}
				}
				
			}
			socket.close();		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			LSimLogger.log(Level.FATAL, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"]" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}	catch (IOException ignored) {}
		
		//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+currentSessionNumber+"] End TSAE session");
	}
}
