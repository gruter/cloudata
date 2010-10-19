/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudata.core.client.browser;

import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.cloudata.core.client.Shell;


class BrowserMenuListener implements ActionListener {
  public void actionPerformed(ActionEvent e) {
    String command = e.getActionCommand();

    if ("Change service id...".equals(command)) {
      changeServiceId();
    } else if ("New query tab".equals(command)) {
      CloudataBrowser.getInstance().addQueryTab();
    } else if ("Exit".equals(command)) {
      System.exit(0);
    } else if ("About...".equals(command)) {

    } else {
      JOptionPane.showMessageDialog(null, "Error", "No listener:" + command,
          JOptionPane.INFORMATION_MESSAGE);
    }
  }

  public void changeServiceId() {
    String option[] = { "Ok", "Cancel" };
    JLabel label1 = new JLabel("Service ID: ", JLabel.RIGHT);
    JTextField fd1 = new JTextField(30);
    JPanel panel = new JPanel(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));

    JPanel namePanel = new JPanel(false);
    namePanel.setLayout(new GridLayout(0, 1));
    namePanel.add(label1);

    JPanel fieldPanel = new JPanel(false);
    fieldPanel.setLayout(new GridLayout(0, 1));
    fieldPanel.add(fd1);

    panel.add(namePanel);
    panel.add(fieldPanel);
    if (JOptionPane.showOptionDialog(CloudataBrowser.frame, panel, "Change Servie ID",
        JOptionPane.YES_NO_OPTION, JOptionPane.INFORMATION_MESSAGE, null,
        option, option[0]) == 0) {
      String serviceId = fd1.getText();
      Shell.conf.set("pleiades.service.name", serviceId);
      
      CloudataBrowser.getInstance().setServiceId(serviceId);
    }
  }
} 
