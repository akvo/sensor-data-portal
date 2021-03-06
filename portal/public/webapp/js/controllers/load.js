/*
 * Kweecker iPad app
 * Author: Neat projects <ties@expertees.nl>
 *
 * Load controller
 */
app.controller('LoadCtrl', function($scope, $rootScope, $location, settings, api, measurements) 
{

	// handle loading of all the settings
	$scope.init = function()
	{
		// start loading the settings, or login
		if (api.token != null)
		{
			settings.fetchSettings();
		}
		else
		{
			$location.path('/login');
		}
	};


	// when the settings are loaded
	$scope.settingsFetched = $rootScope.$on('settingsLoaded', function(e, data)
	{
		// hide splash
		$rootScope.showSplash = false;

		// redirect to the dashboard
		$location.path('/dashboard');

		// start pinging the controller
		//controller.pingConnectionTimerHandler(PING_FREQ_NOT_CONNECTED);
		measurements.initMeasurements();

		// remove this listener
		$scope.settingsFetched();
	});


	// when the settings could not be fetched
	$scope.settingsError = $rootScope.$on('settingsError', function(e, error)
	{
		// check the error
		if (api.token != null)
		{
			// show the error message
			$rootScope.showMessage($rootScope.lang.could_not_load_settings, null, $rootScope.lang.login_title);

			// redirect to the dashboard
			$location.path('/dashboard');
		}
		else
		{
			$location.path('/login');
		}
	});


	// call the init function
	$scope.init();


   	// remove references to the controller
    $scope.removeListeners = function()
    {
		$scope.settingsFetched();
		$scope.settingsError();
    };
    

    $scope.$on('$destroy', function() 
    {
        $scope.removeListeners();
    });

});