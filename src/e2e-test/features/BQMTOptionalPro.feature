Feature: BQMT Optional Properties

  @OPTPROP @DesignTime-TC-ODP-RNTM-01(POC)
  Scenario: User is able to Login and configure the connection parameter to establish the direct connection
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "bqmt.s4PkgSizeOptField"
    When Username "TestUsername" and Password "TestPassword" is provided
