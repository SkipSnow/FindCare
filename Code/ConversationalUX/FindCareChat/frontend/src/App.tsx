// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// Architecture POC host. The React iframe holds capability widgets — each
// widget subscribes to ClientRouter broadcasts and calls router:render to
// paint into the parent's 7 named frames. Widgets are added here as each
// capability is ported.

import HeaderWidget from './components/HeaderWidget'
import MobileNavDrawerWidget from './components/MobileNavDrawerWidget'
import FooterWidget from './components/FooterWidget'
import WelcomeWidget from './components/WelcomeWidget'
import UserPromptWidget from './components/UserPromptWidget'
import SystemMessageWidget from './components/SystemMessageWidget'
import ProviderResultsWidget from './components/ProviderResultsWidget'
import FacilityResultsWidget from './components/FacilityResultsWidget'
import SelectedProvidersWidget from './components/SelectedProvidersWidget'
import ProviderSearchRefinementWidget from './components/ProviderSearchRefinementWidget'
import ProviderDetailWidget from './components/ProviderDetailWidget'
import SpecialtyFilterWidget from './components/SpecialtyFilterWidget'
import SessionDataWidget from './components/SessionDataWidget'
import ContextSwitchWidget from './components/ContextSwitchWidget'
import ClinicalTrialsWidget from './components/ClinicalTrialsWidget'
import SelectedClinicalTrialsWidget from './components/SelectedClinicalTrialsWidget'
import NewQueryLoadingWidget from './components/NewQueryLoadingWidget'
import EvaluateCareSplashWidget from './components/EvaluateCareSplashWidget'
import LegalPanelWidget from './components/LegalPanelWidget'
import OAuthLoginWidget from './components/OAuthLoginWidget'
import FakeGoogleLoginWidget from './components/FakeGoogleLoginWidget'
import AboutChatHealthyWidget from './components/AboutChatHealthyWidget'
import PanelNavWidget from './components/PanelNavWidget'
import PopupHost from './components/PopupHost'

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
      <FakeGoogleLoginWidget />
      <AboutChatHealthyWidget />
      <PanelNavWidget />
    </>
  )
}

export default App
