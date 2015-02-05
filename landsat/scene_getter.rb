require 'date'

module SceneGetter

  # Download a scene from EarthExplorer
  # @param [String] url URL of the scene to download
  # @param [String] save_as Name of file to save scene as
  # @return true, false, or nil as per Kernel.system
  def self.scene(url,save_as)
    login()
#     cmd = "wget -O #{save_as} --no-check-certificate --load-cookies cookies.txt " + url
    cmd = "curl -L -b cookies.txt --insecure -o #{save_as} -O " + url
    system(cmd)
  end



  # Ensures we're "logged into" the site via valid cookie
  # @return nil
  def self.login
    if !File.exist?('cookies.txt') || cookie_outdated?
      get_cookie()
    end
    return nil
  end



  # Is the cookie outdated?
  # @return [Bool]
  def self.cookie_outdated?
    k = ''
    File.readlines('cookies.txt').each do |line|
      if line =~ /\.usgs/
        k = line
        break
      end
    end
    return false if k.empty?
    expiry = Time.at(k.split("\t")[4].to_i)
    # We consider anything within 10s of expiry time to be expired
    return Time.now > (expiry - 10)
  end



  # Gets a new cookie and stores in +cookies.txt+
  # @return true, false, or nil as per Kernel.system
  def self.get_cookie
#     cmd = "wget --no-check-certificate --save-cookies cookies.txt" +
#           " --post-data 'username=rptags&password=iE45nJ9Jkexmd3k0&rememberMe=1&submit='" +
#           " https://earthexplorer.usgs.gov/login/"
    cmd = "curl -c cookies.txt --insecure" +
          " -d 'username=rptags&password=iE45nJ9Jkexmd3k0&rememberMe=1&submit='" +
          " https://earthexplorer.usgs.gov/login/"
    system(cmd)
  end

end # module



# Test:
#SceneGetter.scene('http://earthexplorer.usgs.gov/download/4923/LC80260312014079LGN00/STANDARD/EE')
#SceneGetter.scene('http://earthexplorer.usgs.gov/download/3373/LE70270312011294EDC00/STANDARD/EE')
