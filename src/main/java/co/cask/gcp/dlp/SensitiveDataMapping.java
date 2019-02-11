/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.gcp.dlp;

import com.google.privacy.dlp.v2.InfoType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides a higher level composition of sensitive data.
 *
 * <p>
 *   The class groups underlying DLP {@link InfoType} into a higher level composition
 *   of sensitive data types.
 * </p>
 */
public final class SensitiveDataMapping {
  private final Map<String, String[]> compositeInfoTypes = new HashMap<>();

  public SensitiveDataMapping() {
    compositeInfoTypes.put("DEMOGRAPHIC", new String[]{
      "PERSON_NAME",
      "FIRST_NAME",
      "LAST_NAME",
      "AGE",
      "DATE_OF_BIRTH",
      "GENDER",
      "PHONE_NUMBER",
      "MALE_NAME",
      "FEMALE_NAME",
      "ETHNIC_GROUP",
      "US_STATE",
    });

    compositeInfoTypes.put("LOCATION", new String[]{
      "LOCATION",
      "MAC_ADDRESS",
      "MAC_ADDRESS_LOCAL",
    });

    compositeInfoTypes.put("TAX", new String[]{
      "AUSTRALIA_TAX_FILE_NUMBER",
      "BRAZIL_CPF_NUMBER",
      "DENMARK_CPR_NUMBER",
      "INDIA_PAN_INDIVIDUAL",
      "MEXICO_CURP_NUMBER",
      "NORWAY_NI_NUMBER",
      "PORTUGAL_CDC_NUMBER",
      "UK_NATIONAL_INSURANCE_NUMBER",
      "UK_TAXPAYER_REFERENCE",
      "US_ADOPTION_TAXPAYER_IDENTIFICATION_NUMBER",
      "US_EMPLOYER_IDENTIFICATION_NUMBER",
      "US_INDIVIDUAL_TAXPAYER_IDENTIFICATION_NUMBER",
      "US_PREPARER_TAXPAYER_IDENTIFICATION_NUMBER",
    });

    compositeInfoTypes.put("CREDIT_CARD", new String[]{
      "CREDIT_CARD_NUMBER",
    });

    compositeInfoTypes.put("PASSPORT", new String[]{
      "CANADA_PASSPORT",
      "CHINA_PASSPORT",
      "FRANCE_PASSPORT",
      "GERMANY_PASSPORT",
      "JAPAN_PASSPORT",
      "KOREA_PASSPORT",
      "MEXICO_PASSPORT",
      "NETHERLANDS_PASSPORT",
      "POLAND_PASSPORT",
      "SPAIN_PASSPORT",
      "SWEDEN_PASSPORT",
      "UK_PASSPORT",
      "US_PASSPORT",
    });

    compositeInfoTypes.put("HEALTH", new String[]{
      "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
      "US_DEA_NUMBER",
      "US_HEALTHCARE_NPI",
      "CANADA_BC_PHN",
      "CANADA_OHIP",
      "CANADA_QUEBEC_HIN",
      "CANADA_SOCIAL_INSURANCE_NUMBER",
    });

    compositeInfoTypes.put("NATIONAL_ID", new String[]{
      "ARGENTINA_DNI_NUMBER",
      "CHILE_CDI_NUMBER",
      "CHINA_RESIDENT_ID_NUMBER",
      "COLOMBIA_CDC_NUMBER",
      "DENMARK_CPR_NUMBER",
      "FRANCE_CNI",
      "FRANCE_NIR",
      "FINLAND_NATIONAL_ID_NUMBER",
      "JAPAN_INDIVIDUAL_NUMBER",
      "MEXICO_CURP_NUMBER",
      "NETHERLANDS_BSN_NUMBER",
      "NORWAY_NI_NUMBER",
      "PARAGUAY_CIC_NUMBER",
      "PERU_DNI_NUMBER",
      "POLAND_PESEL_NUMBER",
      "POLAND_NATIONAL_ID_NUMBER",
      "PORTUGAL_CDC_NUMBER",
      "SPAIN_NIE_NUMBER",
      "SPAIN_NIF_NUMBER",
      "SWEDEN_NATIONAL_ID_NUMBER",
      "US_SOCIAL_SECURITY_NUMBER",
      "URUGUAY_CDI_NUMBER",
      "VENEZUELA_CDI_NUMBER",
    });

    compositeInfoTypes.put("DRIVER_LICENSE", new String[]{
      "CANADA_DRIVERS_LICENSE_NUMBER",
      "JAPAN_DRIVERS_LICENSE_NUMBER",
      "SPAIN_DRIVERS_LICENSE_NUMBER",
      "UK_DRIVERS_LICENSE_NUMBER",
      "US_DRIVERS_LICENSE_NUMBER",
    });

    compositeInfoTypes.put("INSURANCE", new String[] {
      "CANADA_SOCIAL_INSURANCE_NUMBER",
      "UK_NATIONAL_INSURANCE_NUMBER",
      "CANADA_SOCIAL_INSURANCE_NUMBER",
    });
  };

  /**
   * Returns the Dlp list of {@link InfoType} for all the composite sensitive data.
   *
   * @param types list of sensitive data types specified by user.
   * @return List of {@link InfoType} that need to be idenfified in the data.
   */
  public List<InfoType> getSensitiveInfoTypes(String[] types) {
    List<InfoType> selectedInfoTypes = new ArrayList<>();
    for (String type : types) {
      if (compositeInfoTypes.containsKey(type)) {
        String[] infoTypeNames = compositeInfoTypes.get(type);
        for (String infoTypeName : infoTypeNames) {
          selectedInfoTypes.add(
            InfoType.newBuilder().setName(infoTypeName).build()
          );
        }
      }
    }
    return selectedInfoTypes;
  }
}
