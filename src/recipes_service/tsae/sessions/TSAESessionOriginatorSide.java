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
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionOriginatorSide extends TimerTask {
	private static final AtomicInteger SESSION_NUMBER = new AtomicInteger(0);
	
	private final ServerData serverData;

	public TSAESessionOriginatorSide(ServerData serverData){
		super();
		this.serverData=serverData;		
	}
	
	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run(){
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * @param num
	 */
	public void sessionWithN(int num){
		if(!SimulationData.getInstance().isConnected()) {
			return;
		}

		List<Host> partnersTSAESession = serverData.getRandomPartners(num);
    for (Host host: partnersTSAESession) {
      sessionTSAE(host);
    }
	}
	
	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * @param n
	 */
	private void sessionTSAE(Host n){
		int current_session_number = SESSION_NUMBER.incrementAndGet();
		if (n == null) return;
		
		LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] TSAE session");
		
		try {
			Socket socket = new Socket(n.getAddress(), n.getPort());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

			TimestampVector localSummary;
			TimestampMatrix localAck;

			// Get the local summary and local ack before sending to partner
			synchronized (serverData) {
				localSummary = serverData.getSummary().clone();
				serverData.getAck().update(serverData.getId(), localSummary);
				localAck = serverData.getAck().clone();
			}

			// Send to partner: local's summary and ack
			Message	msg = new MessageAErequest(localSummary, localAck);
			msg.setSessionNumber(current_session_number);
			out.writeObject(msg);
			//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

			// receive operations from partner
			msg = (Message) in.readObject();
			//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
			while (msg.type() == MsgType.OPERATION) {
				MessageOperation operationMsg = (MessageOperation) msg;
				synchronized (serverData) {
					serverData.performOperation(operationMsg.getOperation());
				}

				msg = (Message) in.readObject();
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
			}

			// receive partner's summary and ack
			if (msg.type() == MsgType.AE_REQUEST){
				MessageAErequest partner = (MessageAErequest) msg;

				// send operations
				for (Operation operation: serverData.getLog().listNewer(partner.getSummary())) {
					MessageOperation operationMsg = new MessageOperation(operation);
					operationMsg.setSessionNumber(current_session_number);
					out.writeObject(operationMsg);
					//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);
				}

				// send and "end of TSAE session" message
				MessageEndTSAE endTSAEMsg = new MessageEndTSAE();
				endTSAEMsg.setSessionNumber(current_session_number);
				out.writeObject(endTSAEMsg);
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

				// receive message to inform about the ending of the TSAE session
				msg = (Message) in.readObject();
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
				if (msg.type() == MsgType.END_TSAE){
					synchronized (serverData) {
						serverData.getSummary().updateMax(partner.getSummary());
						serverData.getAck().updateMax(partner.getAck());
						serverData.getLog().purgeLog(serverData.getAck());
					}
				}

			}			
			socket.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			LSimLogger.log(Level.FATAL, "[TSAESessionOriginatorSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}catch (IOException e) {}

		
		//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] End TSAE session");
	}
}