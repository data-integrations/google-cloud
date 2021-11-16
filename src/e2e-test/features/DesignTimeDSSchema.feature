Feature: Fetch schema of all the datasources active in SAP

  @DesignTime-TC-ODP-DSGN-01.05
  Scenario Outline: User is able to get the schema of the datasources supporting all the datatype
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "S4client" "S4sysnr" "S4asHost" "S4dsName" "S4gcsPath" "S4Splitrow" "S4pkgSize"
    When Username "TestUsername" and Password "TestPassword" is provided
    When data source as "<datasource>" is added
    Then Validate that schema is created
    Examples:
      |datasource|
      |2LIS_02_ITM|
      |2LIS_11_VAITM|
      |0MATERIAL_LPRH_HIER             |