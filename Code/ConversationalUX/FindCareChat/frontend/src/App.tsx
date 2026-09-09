// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// Architecture POC host. The React iframe holds capability widgets — each
// widget subscribes to ClientRouter broadcasts and calls router:render to
// paint into the parent's 7 named frames. Widgets are added here as each
// capability is ported.

import HeaderWidget from './components/HeaderWidget'
import MobileNavDrawerWidget from '../../../../../sharedServices/Code/displayChrome/MobileNavDrawerWidget'
import FooterWidget from '../../../../../sharedServices/Code/displayChrome/FooterWidget'
import WelcomeWidget from '../../../../../sharedServices/Code/displayChrome/WelcomeWidget'
import UserPromptWidget from '../../../../../sharedServices/Code/UtteranceManager/UserPromptWidget'
import SystemMessageWidget from '../../../../../sharedServices/Code/UtteranceManager/SystemMessageWidget'
import ProviderResultsWidget from '../../../../../FindCare/ProviderManagement/ProviderResultsWidget'
import FacilityResultsWidget from '../../../../../sharedServices/Code/FacilitySearch/FacilityResultsWidget'
import SelectedProvidersWidget from './components/SelectedProvidersWidget'
import ProviderSearchRefinementWidget from './components/ProviderSearchRefinementWidget'
import ProviderDetailWidget from '../../../../../FindCare/ProviderDetail/ProviderDetailWidget'
import SpecialtyFilterWidget from '../../../../../FindCare/SpecialtyFilter/SpecialtyFilterWidget'
import SessionDataWidget from '../../../../../sharedServices/Code/AuthorizationsAndAuthentications/SessionDataWidget'
import ContextSwitchWidget from '../../../../../sharedServices/Code/externalInterface/ContextSwitchWidget'
import ClinicalTrialsWidget from './components/ClinicalTrialsWidget'
import SelectedClinicalTrialsWidget from '../../../../../sharedServices/Code/ClinicalTrialSelection/SelectedClinicalTrialsWidget'
import NewQueryLoadingWidget from '../../../../../sharedServices/Code/crossComponentTimers/NewQueryLoadingWidget'
import EvaluateCareSplashWidget from '../../../../../sharedServices/Code/handoffToEvaluateCare/EvaluateCareSplashWidget'
import LegalPanelWidget from './components/LegalPanelWidget'
import OAuthLoginWidget from '../../../../../sharedServices/Code/AuthorizationsAndAuthentications/OAuthLoginWidget'
import AboutChatHealthyWidget from '../../../../../sharedServices/Code/AboutChatHealthy/AboutChatHealthyWidget'
import PanelNavWidget from '../../../../../sharedServices/Code/displayChrome/PanelNavWidget'
import PopupHost from '../../../../../sharedServices/Code/displayChrome/PopupHost'

function App() {
  return (
    <>
      <PopupHost />
      <HeaderWidget />
      <MobileNavDrawerWidget />
      <FooterWidget />
      <WelcomeWidget />
      <UserPromptWidget />
      <SystemMessageWidget />
      <ProviderResultsWidget />
      <FacilityResultsWidget />
      <SelectedProvidersWidget />
      <ProviderSearchRefinementWidget />
      <ProviderDetailWidget />
      <SpecialtyFilterWidget />
      <SessionDataWidget />
      <ContextSwitchWidget />
      <ClinicalTrialsWidget />
      <SelectedClinicalTrialsWidget />
      <NewQueryLoadingWidget />
      <EvaluateCareSplashWidget />
      <LegalPanelWidget />
      <OAuthLoginWidget />
      <AboutChatHealthyWidget />
      <PanelNavWidget />
    </>
  )
}

export default App
