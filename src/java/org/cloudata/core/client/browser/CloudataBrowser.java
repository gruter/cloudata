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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.border.BevelBorder;

import org.cloudata.core.client.Shell;


public class CloudataBrowser {
  /** int initWidth : 윈도우 초기넓이 */
  final int initWidth = 800;

  /** int initHeight : 윈도우 초기높이 */
  final int initHeight = 600;

  /** 메인 윈도우 프레임 */
  static JFrame frame;

  /** Query를 수행시키는 Panel */
  List<QueryPanel> queryPanels = new ArrayList<QueryPanel>();
  
  TableSchemaPanel tableSchemaPanel;

  /** 화면 하단의 Copyright 표시 Label */
  JLabel copyrightBar;

  JLabel cloudataServiceBar;

  /** 처리결과를 나타내는 StatusBar */
  JLabel statusBar = null;

  /** copyright 문자열 */
  final String copyRight = "Cloudata browser";

  
  static CloudataBrowser instance;
  
  JTabbedPane tabbedPane;
  
  int queryTabTitle = 1;
  
  /* DBExplorer의 메인 메소드, 생성자만 호출한다. */
  public static void main(String args[]) {
    getInstance();
  }

  public synchronized static CloudataBrowser getInstance() {
    if(instance == null) {
      instance = new CloudataBrowser();
    }
    
    return instance;
  }
  
  /** 화면하단의 상태바를 설정한다. */
  public void setStatusBar(String msg) {
    statusBar.setText(msg);
  }

  /** 생성자 : 메인프레임, 각 팬널 구성 */
  public CloudataBrowser() {
    //setTheme();
    frame = new BrowserFrame("Cloudata Browser");
    // ///////////////////////////////////////////////////////////////
    // 윈도우 종료 이벤트 처리
    frame.addWindowListener(new WindowAdapter() {
      public void windowClosing(WindowEvent e) {
        System.exit(0);
      }
    });
    // ///////////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////////
    // 메인 프레임에 붙을 Panel 설정
    JPanel bigPanel = new JPanel();
    bigPanel.setLayout(new BorderLayout());
    // ///////////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////////
    // 화면 오른쪽의 Tab 생성
    tabbedPane = new JTabbedPane();

    addQueryTab();
    tableSchemaPanel = new TableSchemaPanel();
    tabbedPane.add("Schema", tableSchemaPanel);
    tabbedPane.setMinimumSize(new Dimension(0, 0));

    bigPanel.add(tabbedPane, BorderLayout.CENTER);

    // ///////////////////////////////////////////////////////////////
    // 화면 하단의 상태 바 설정
    JPanel statusPanel = new JPanel();
    statusPanel.setLayout(new GridLayout(1, 3));

    statusBar = new JLabel();
    statusBar.setHorizontalAlignment(SwingConstants.RIGHT);
    statusBar.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
    setStatusBar("Query를 입력하세요.");

    cloudataServiceBar = new JLabel("Service ID: " + Shell.conf.get("pleiades.service.name"));
    cloudataServiceBar.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
    cloudataServiceBar.setHorizontalAlignment(SwingConstants.CENTER);

    copyrightBar = new JLabel(copyRight);
    copyrightBar.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
    copyrightBar.setHorizontalAlignment(SwingConstants.LEFT);

    statusPanel.add(copyrightBar);
    statusPanel.add(cloudataServiceBar);
    statusPanel.add(statusBar);
    // ///////////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////////
    // 메뉴 만들기
    JMenuBar jmb = setMenu();
    frame.setJMenuBar(jmb);
    // ///////////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////////
    // 프레임에 붙이기
    frame.getContentPane().add("Center", bigPanel);
    frame.getContentPane().add("South", statusPanel);
    // ///////////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////////
    // 프레임 보이기
    Dimension dim = frame.getToolkit().getScreenSize();

    frame.setSize(initWidth, initHeight);
    frame.setLocation((dim.width - initWidth) / 2,
        (dim.height - initHeight) / 2);

    frame.setVisible(true);
    // ///////////////////////////////////////////////////////////////
  }

  // private void fileDelete() {
  // try {
  // Runtime.getRuntime().exec(new String[] { "delexcelfile.bat" });
  // } catch (Exception e) {
  // System.out.println("엑셀 임시 파일 삭제 오류>" + e.toString());
  // }
  // }

  /** 메뉴를 설정한다 */
  protected JMenuBar setMenu() {
    JMenuBar jmb = new JMenuBar();
    JMenuItem item;

    // 메뉴 이벤트 처리 클래스를 생성한다.
    BrowserMenuListener menulistener = new BrowserMenuListener();

    // ////////////////////////////////////////////////////////
    // File 메뉴
    // ////////////////////////////////////////////////////////
    JMenu fileMenu = new JMenu("File");

    item = new JMenuItem("Change service id...");
    item.addActionListener(menulistener);
    item.setHorizontalTextPosition(JMenuItem.RIGHT);
    fileMenu.add(item);

    item = new JMenuItem("New query tab");
    item.addActionListener(menulistener);
    fileMenu.add(item);
    item.setHorizontalTextPosition(JMenuItem.RIGHT);
    
    fileMenu.addSeparator();

    item = new JMenuItem("Exit");
    item.addActionListener(menulistener);
    fileMenu.add(item);
    item.setHorizontalTextPosition(JMenuItem.RIGHT);

    jmb.add(fileMenu);
    // /////////////////////////////////////////////////////////

    // ////////////////////////////////////////////////////////
    // Help 관리 메뉴
    // ////////////////////////////////////////////////////////
    JMenu helpMenu = new JMenu("Help");

    item = new JMenuItem("About...");
    item.addActionListener(menulistener);
    helpMenu.add(item);
    item.setHorizontalTextPosition(JMenuItem.RIGHT);

    jmb.add(helpMenu);
    // /////////////////////////////////////////////////////////

    return jmb;
  }

  public static void setTheme() {
    int fontSize = 11;

    try {
      UIManager
          .setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel");
    } catch (Exception ex) {
    }

    Font dialogPlain = new Font("Dialog", Font.PLAIN, fontSize);
    Font dialogBold = new Font("Dialog", Font.PLAIN, 12);
    Font serifPlain = new Font("Serif", Font.PLAIN, fontSize);
    Font sansSerifPlain = new Font("SansSerif", Font.PLAIN, fontSize);
    Font monospacedPlain = new Font("Monospaced", Font.PLAIN, fontSize);

    UIManager.getDefaults().put("JButton.font", dialogPlain);
    UIManager.getDefaults().put("ToggleJButton.font", dialogPlain);
    UIManager.getDefaults().put("RadioJButton.font", dialogPlain);
    UIManager.getDefaults().put("CheckBox.font", dialogPlain);
    UIManager.getDefaults().put("ColorChooser.font", dialogPlain);
    UIManager.getDefaults().put("ComboBox.font", dialogPlain);
    UIManager.getDefaults().put("Label.font", dialogPlain);
    UIManager.getDefaults().put("List.font", dialogPlain);
    UIManager.getDefaults().put("MenuBar.font", dialogBold);
    UIManager.getDefaults().put("MenuItem.font", dialogBold);
    UIManager.getDefaults().put("RadioJButtonMenuItem.font", dialogPlain);
    UIManager.getDefaults().put("CheckBoxMenuItem.font", dialogPlain);
    UIManager.getDefaults().put("Menu.font", dialogBold);
    UIManager.getDefaults().put("PopupMenu.font", dialogPlain);
    UIManager.getDefaults().put("OptionPane.font", dialogPlain);
    UIManager.getDefaults().put("Panel.font", dialogPlain);
    UIManager.getDefaults().put("ProgressBar.font", dialogPlain);
    UIManager.getDefaults().put("ScrollPane.font", dialogPlain);
    UIManager.getDefaults().put("Viewport.font", dialogPlain);
    UIManager.getDefaults().put("TabbedPane.font", dialogPlain);
    UIManager.getDefaults().put("Table.font", dialogPlain);
    UIManager.getDefaults().put("TableHeader.font", dialogPlain);
    UIManager.getDefaults().put("TextField.font", sansSerifPlain);
    UIManager.getDefaults().put("PasswordField.font", monospacedPlain);
    UIManager.getDefaults().put("TextArea.font", monospacedPlain);
    UIManager.getDefaults().put("TextPane.font", serifPlain);
    UIManager.getDefaults().put("EditorPane.font", serifPlain);
    UIManager.getDefaults().put("TitledBorder.font", dialogPlain);
    UIManager.getDefaults().put("ToolBar.font", dialogPlain);
    UIManager.getDefaults().put("ToolTip.font", sansSerifPlain);
    UIManager.getDefaults().put("Tree.font", dialogPlain);

//    EmptyBorder border = new EmptyBorder(0, 0, 0, 0);
//    UIManager.getDefaults().put("SplitPane.border", border);
  }

  public void setServiceId(String serviceId) {
    cloudataServiceBar.setText("Service ID: " + Shell.conf.get("pleiades.service.name"));    
    tableSchemaPanel.reload();
  }

  public void removeQueryTab() {
    int index = tabbedPane.getSelectedIndex();
    QueryPanel queryPanel = queryPanels.remove(index);
    tabbedPane.remove(index);
    queryPanel.closePanel();
    
    if(queryPanels.size() == 1) {
      queryPanel = queryPanels.get(0);
      queryPanel.setCloseButtonEnable(false);
    }
  }

  public void addQueryTab() {
    QueryPanel queryPanel = new QueryPanel();
    queryPanels.add(queryPanel);
    
    tabbedPane.add(queryPanel, "Query(" + queryTabTitle + ")", queryPanels.size() - 1);
    queryTabTitle++;
    
    tabbedPane.setSelectedIndex(queryPanels.size() - 1);
    boolean enable = queryPanels.size() > 1 ? true : false;
    
    for(QueryPanel eachQueryPanel: queryPanels) {
      eachQueryPanel.setCloseButtonEnable(enable);
    }
  }
}
