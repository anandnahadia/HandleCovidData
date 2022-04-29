package covidData

type covidDataService interface {
	updateStatesCovidData(logger kitlog.Logger) error
	getStateCovidData(logger kitlog.Logge, input Input) error
}

type CovidData struct {
}

func (c CovidData) updateStatesCovidData(logger kitlog.Logger) error {
	return nil
}
func (c CovidData) getStateCovidData(logger kitlog.Logge, input Input) error {
	return nil
}
