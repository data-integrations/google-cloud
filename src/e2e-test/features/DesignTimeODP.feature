Feature: Design Time ODP Scenario

  @ODP @DesignTime-TC-ODP-DSGN-01(Direct)
  Scenario:User configured direct connection parameters and Security parameters by providing values on SAP UI(ENV)
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username "TestUsername" and Password "TestPassword" is provided
    Then Connection is established

  @ODP @DesignTime-TC-ODP-DSGN-01(LOAD)
  Scenario:User configured Load connection parameters and Security parameters by providing values on SAP UI(ENV)
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When LoadProp "S4client" "S4asHost" "S4msServ" "S4systemID" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize" "S4Lgrp"
    When Username "TestUsername" and Password "TestPassword" is provided
    Then Connection is established

  @DesignTime-TC-ODP-DSGN-01.05
  Scenario: User is able to configure Security parameters using macros in direct connection
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username "TestUsername" and Password "TestPassword" is provided
    When User has selected Sap client macro to configure
    Then User is validate without any error
    When User has selected Sap language macro to configure
    Then User is validate without any error
    When User has selected Sap server as host macro to configure
    Then User is validate without any error
    When User has selected System Number macro to configure
    Then User is validate without any error
    When User has selected datasource macro to configure
    Then User is validate without any error
    When User has selected gcsPath macro to configure
    Then User is validate without any error

  @DesignTime-TC-ODP-DSGN-01.05
  Scenario: User is able to configure Security parameters using macros in load connection
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When LoadProp "S4client" "S4msHost" "S4msServ" "S4systemID" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize" "S4Lgrp"
    When Username "TestUsername" and Password "TestPassword" is provided
    When User has selected Sap msHost macro to configure
    Then User is validate without any error
    When User has selected Sap msServ macro to configure
    Then User is validate without any error
    When User has selected UserName and Password macro to configure
    Then User is validate without any error



